import copy
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import tkinter as tk
from datetime import datetime, timedelta, timezone
from tkinter import ttk, messagebox, scrolledtext

from scheduler_control import PID_FILE, read_control, write_control, is_service_running
from manager import FTPSiteManager
from models import MissingFilesLog
from notifier import (load_notifications, save_notifications, load_smtp, save_smtp,
                      send_notification, notify_ping_alert)

logger = logging.getLogger(__name__)

def extract_station_name(filename):
    match = re.search(r'([A-Z]{4}\d{2}[A-Z])', filename.upper())
    return match.group(1) if match else "UNKNOWN"

def format_size(bytes_val):
    if bytes_val <= 0: return "—"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024: return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} TB"

class FTPSiteGUI:
    def __init__(self, manager):
        self.manager = manager
        self.root = tk.Tk()
        self.root.title("DGnet FTP Monitor")
        self.root.geometry("1400x900")
        self.root.minsize(900, 600)
        self.days_var = tk.IntVar(value=1)
        self.all_remote_var = tk.BooleanVar(value=False)
        self.show_issues = tk.BooleanVar(value=True)
        self.filter_site = tk.StringVar(value="All Stations")
        self.summary_filter = tk.StringVar(value="All Stations")
        self.auto_refresh = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="Ready")
        self.scheduler_var = tk.StringVar(value="Scheduler: Stopped")
        self.delay_minutes = tk.IntVar(value=15)
        self.full_log = None
        self.col_filters = {'Network': set(), 'Station': set(), 'Log Name': set()}
        self.scheduler_running = False
        self.scheduler_thread = None
        self.next_run_time = None
        self.missing_text = None
        self._missing_by_iid = {}
        self._log_pos = 0
        self.mount_tree = None
        self.ping_tree = None
        self._ping_after_id = None
        self.ping_enabled_var = None
        self._prev_ping_state = {}   # host -> bool (last known reachability)
        self._build_ui()
        self._init_log_pos()
        self._refresh_sites()
        self._check_service_on_start()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _check_service_on_start(self):
        """If the scheduler service is already running, sync the GUI state."""
        if is_service_running():
            ctrl = read_control()
            if ctrl and ctrl.get('running', False):
                self.scheduler_running = True
                self.delay_minutes.set(ctrl.get('delay', 15))
                self.days_var.set(ctrl.get('days', 7))
                self.scheduler_btn.config(text="STOP SCHEDULER")
                self.led.delete("dot")
                self.led.create_oval(4, 4, 14, 14, fill="lime", outline="green", width=3, tags="dot")
                self._schedule_next_run()
                self.scheduler_thread = threading.Thread(
                    target=self._scheduler_loop, args=(False,), daemon=True)
                self.scheduler_thread.start()
                logger.info("Detected running scheduler service (PID in %s)", PID_FILE)

    def _build_ui(self):
        paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill='both', expand=True)

        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=1)

        ttk.Label(left_frame, text="Greek GNSS Network - Grouped View", font=('Arial', 16, 'bold')).pack(pady=15)
        self.tree_sites = ttk.Treeview(left_frame, columns=('name', 'chk'), show='tree headings', selectmode='extended')
        self.tree_sites.column('#0', stretch=True, minwidth=80)
        self.tree_sites.column('name', width=0, stretch=False, minwidth=0)
        self.tree_sites.column('chk', width=40, stretch=False, anchor='center', minwidth=40)
        self.tree_sites.heading('#0', text='')
        self.tree_sites.heading('name', text='')
        self.tree_sites.heading('chk', text='')
        self.tree_sites.pack(fill='both', expand=True, padx=20, pady=10)
        self.tree_sites.tag_configure('leaf_enabled', font=('Segoe UI', 11))
        self.tree_sites.tag_configure('leaf_disabled', foreground='#aaaaaa', font=('Segoe UI', 11))
        self.tree_sites.bind('<<TreeviewSelect>>', self._on_tree_select)
        self.tree_sites.bind('<ButtonRelease-1>', self._on_tree_click)
        
        btns = ttk.Frame(left_frame)
        btns.pack(pady=8)
        ttk.Button(btns, text="Add Station", command=self._add_site).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Edit", command=self._edit_site).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Delete", command=self._delete_site).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Collapse All", command=lambda: [self.tree_sites.item(i, open=False) for i in self.tree_sites.get_children()]).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Expand All", command=lambda: [self.tree_sites.item(i, open=True) for i in self.tree_sites.get_children()]).pack(side=tk.LEFT, padx=6)

        right = ttk.Frame(paned)
        paned.add(right, weight=5)

        self.notebook = ttk.Notebook(right)
        self.notebook.pack(fill='both', expand=True, padx=15, pady=10)

        # TAB 1: File Monitor
        tab1 = ttk.Frame(self.notebook)
        self.notebook.add(tab1, text=" File Monitor ")

        ctrl = ttk.LabelFrame(tab1, text=" Controls - Greek National Standard v9.999.9.7 ")
        ctrl.pack(fill='x', padx=10, pady=8)

        row1 = ttk.Frame(ctrl); row1.pack(fill='x', pady=6)
        ttk.Label(row1, text="Days:").pack(side=tk.LEFT, padx=5)
        self.days_spin = ttk.Spinbox(row1, from_=1, to=365, textvariable=self.days_var, width=5)
        self.days_spin.pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(row1, text="All available (remote)", variable=self.all_remote_var,
                        command=self._on_all_remote_toggle).pack(side=tk.LEFT, padx=8)
        ttk.Checkbutton(row1, text="Issues only", variable=self.show_issues, command=self._filter_only).pack(side=tk.LEFT, padx=20)
        ttk.Label(row1, text="Station:").pack(side=tk.LEFT, padx=10)
        self.combo = ttk.Combobox(row1, textvariable=self.filter_site, state='readonly', width=30)
        self.combo.pack(side=tk.LEFT, padx=5)
        self.combo.bind('<<ComboboxSelected>>', lambda e: self._filter_only())
        ttk.Checkbutton(row1, text="Auto-Refresh after download", variable=self.auto_refresh).pack(side=tk.LEFT, padx=30)

        row2 = ttk.Frame(ctrl); row2.pack(fill='x', pady=8)
        self.scan_btn = ttk.Button(row2, text="SCAN", command=self._refresh_table, style="Accent.TButton")
        self.scan_btn.pack(side=tk.LEFT, padx=5)
        self.dl_btn = ttk.Button(row2, text="Download Completed Files", command=self._download)
        self.dl_btn.pack(side=tk.LEFT, padx=5)
        self.scheduler_btn = ttk.Button(row2, text="START SCHEDULER", command=self._toggle_scheduler)
        self.scheduler_btn.pack(side=tk.LEFT, padx=10)
        self.led = tk.Canvas(row2, width=18, height=18, bg="#f0f0f0", highlightthickness=0)
        self.led.pack(side=tk.LEFT, padx=6)
        self.led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
        self.dl_led = tk.Canvas(row2, width=18, height=18, bg="#f0f0f0", highlightthickness=0)
        self.dl_led.pack(side=tk.LEFT, padx=(0, 6))
        self.dl_led.create_oval(4, 4, 14, 14, fill="#d0d0d0", tags="dot")
        ttk.Label(row2, text="Minutes after hour:").pack(side=tk.LEFT, padx=8)
        ttk.Spinbox(row2, from_=1, to=59, textvariable=self.delay_minutes, width=5).pack(side=tk.LEFT, padx=5)

        ttk.Label(ctrl, textvariable=self.scheduler_var, foreground="#006400", font=('Segoe UI', 10, 'bold'), anchor='w').pack(fill='x', padx=15, pady=4)
        self.progress = ttk.Progressbar(ctrl, mode='determinate')
        self.progress.pack(fill='x', padx=15, pady=8)

        columns = ('Network','Station','Log Name','Freq','Date (UTC)','File','Local','Local Size','Remote','Remote Size','Status','Type')
        tree_frame = ttk.Frame(tab1)
        tree_frame.pack(fill='both', expand=True, padx=15, pady=10)
        h_scroll = ttk.Scrollbar(tree_frame, orient='horizontal')
        v_scroll = ttk.Scrollbar(tree_frame, orient='vertical')
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings',
                                 xscrollcommand=h_scroll.set, yscrollcommand=v_scroll.set)
        h_scroll.config(command=self.tree.xview)
        v_scroll.config(command=self.tree.yview)
        h_scroll.pack(side='bottom', fill='x')
        v_scroll.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        widths = [70,80,100,65,150,380,60,90,60,90,160,100]
        _filterable = ('Network', 'Station', 'Log Name')
        for c, w in zip(columns, widths):
            if c in _filterable:
                self.tree.heading(c, text=c, command=lambda col=c: self._show_col_filter(col))
            else:
                self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor='center')
        self.tree.column('File', anchor='w')
        self.tree.column('Log Name', anchor='w')
        self.tree.column('Freq', anchor='center')
        self.tree.column('Local Size', anchor='e')
        self.tree.column('Remote Size', anchor='e')

        self.tree.tag_configure('missing_local', background='#ffcccc', foreground='red')
        self.tree.tag_configure('missing_remote', background='#ffffcc', foreground='darkorange')
        self.tree.tag_configure('mismatch', background='#ff9999', foreground='darkred')
        self.tree.tag_configure('ok', background='#ccffcc', foreground='darkgreen')
        self.tree.tag_configure('scheduled', background='#cce5ff', foreground='#0055aa')
        self.tree.tag_configure('current_growing', background='#e6f3ff', foreground='blue', font=('Segoe UI', 9, 'bold'))
        self.tree.tag_configure('conn_failed', background='#e0e0e0', foreground='#666666')

        # TAB 2: Network Summary
        tab2 = ttk.Frame(self.notebook)
        self.notebook.add(tab2, text=" Network Summary ")

        sum_ctrl = ttk.LabelFrame(tab2, text=" Summary Settings ")
        sum_ctrl.pack(fill='x', padx=15, pady=10)
        ttk.Label(sum_ctrl, text="Show:").pack(side=tk.LEFT, padx=10)
        self.summary_combo = ttk.Combobox(sum_ctrl, textvariable=self.summary_filter, state='readonly', width=35)
        self.summary_combo.pack(side=tk.LEFT, padx=5)
        self.summary_combo.bind('<<ComboboxSelected>>', lambda e: self._refresh_summary())
        ttk.Button(sum_ctrl, text="REFRESH SUMMARY", command=self._refresh_summary, style="Accent.TButton").pack(side=tk.LEFT, padx=20)
        ttk.Checkbutton(sum_ctrl, text="Auto-Refresh after download", variable=self.auto_refresh).pack(side=tk.LEFT, padx=30)

        # Colour legend
        legend = ttk.Frame(sum_ctrl)
        legend.pack(side=tk.RIGHT, padx=10)
        for colour, fg, text in [
            ('#ccffcc', 'darkgreen',  'All files present'),
            ('#fff3cc', 'darkorange', 'Older gaps only — latest file OK or awaiting download'),
            ('#ffcccc', 'red',        'Latest expected file missing'),
        ]:
            cell = tk.Frame(legend, bg=colour, bd=1, relief='solid')
            cell.pack(side=tk.LEFT, padx=3)
            tk.Label(cell, text=f'  {text}  ', bg=colour, fg=fg,
                     font=('Segoe UI', 8)).pack()

        sum_paned = ttk.PanedWindow(tab2, orient=tk.HORIZONTAL)
        sum_paned.pack(fill='both', expand=True, padx=15, pady=10)

        tree_frame_sum = ttk.Frame(sum_paned)
        sum_paned.add(tree_frame_sum, weight=1)
        h_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient='horizontal')
        self.summary_tree = ttk.Treeview(tree_frame_sum, columns=('Network','Station','Site','Last Download','Last File','Missing Count'),
                                        show='headings', xscrollcommand=h_scroll_sum.set)
        h_scroll_sum.config(command=self.summary_tree.xview)
        h_scroll_sum.pack(side='bottom', fill='x')
        self.summary_tree.pack(fill='both', expand=True)

        widths_sum = [90,100,200,180,220,110]
        for c, w in zip(self.summary_tree['columns'], widths_sum):
            self.summary_tree.heading(c, text=c)
            self.summary_tree.column(c, width=w, anchor='center')
        self.summary_tree.column('Network', anchor='w')
        self.summary_tree.column('Station', anchor='w')
        self.summary_tree.column('Site', anchor='w')

        missing_frame = ttk.LabelFrame(sum_paned, text=" ROLLING MISSING FILES LIST")
        sum_paned.add(missing_frame, weight=1)
        self.missing_text = scrolledtext.ScrolledText(missing_frame, width=70, height=30, font=('Consolas', 10), bg='#fff8f8', wrap='none')
        h_scroll_text = ttk.Scrollbar(missing_frame, orient='horizontal', command=self.missing_text.xview)
        self.missing_text.configure(xscrollcommand=h_scroll_text.set)
        self.missing_text.pack(fill='both', expand=True, padx=10, pady=(10,0))
        h_scroll_text.pack(fill='x', padx=10, pady=(0,10))

        self.summary_tree.tag_configure('ok',      foreground='darkgreen')
        self.summary_tree.tag_configure('warning', foreground='darkorange', font=('Segoe UI', 9, 'bold'))
        self.summary_tree.tag_configure('missing', foreground='red',        font=('Segoe UI', 9, 'bold'))
        self.summary_tree.bind('<<TreeviewSelect>>', self._show_missing_details)

        # TAB 3: Notifications
        tab3 = ttk.Frame(self.notebook)
        self.notebook.add(tab3, text=" Notifications ")
        self._build_notifications_tab(tab3)

        # TAB 4: Checkings
        tab4 = ttk.Frame(self.notebook)
        self.notebook.add(tab4, text=" Checking ")
        self._build_checkings_tab(tab4)

        status_frame = ttk.Frame(self.root)
        status_frame.pack(fill='x', side='bottom')
        self.status_text = tk.Text(status_frame, height=2, font=('Segoe UI', 10),
                                   relief='sunken', state='disabled', wrap='word',
                                   bg='#f0f0f0', padx=10, pady=5, bd=1)
        self.status_text.pack(fill='x')
        self.status_var.trace_add('write', self._update_status_display)

        style = ttk.Style()
        style.configure("Accent.TButton", foreground="white", background="#0066cc", font=('Segoe UI', 10, 'bold'))
        style.configure("Treeview", rowheight=26, font=('Consolas', 10))

    def _build_notifications_tab(self, parent):
        cfg = load_notifications()
        self._notif_rows = []  # list of dicts: {email_var, on_start, on_stop, on_inactive, on_download}

        top = ttk.Frame(parent)
        top.pack(fill='x', padx=20, pady=(15, 5))
        self.notif_enabled = tk.BooleanVar(value=cfg.get('enabled', False))
        ttk.Checkbutton(top, text="Enable email notifications", variable=self.notif_enabled,
                        command=self._save_notifications).pack(anchor='w')

        # Table with header row
        tbl_outer = ttk.LabelFrame(parent, text=" Recipients & Events ")
        tbl_outer.pack(fill='both', expand=True, padx=20, pady=5)

        # Scrollable canvas — vertical + horizontal
        canvas = tk.Canvas(tbl_outer, highlightthickness=0)
        v_sb = ttk.Scrollbar(tbl_outer, orient='vertical',   command=canvas.yview)
        h_sb = ttk.Scrollbar(tbl_outer, orient='horizontal', command=canvas.xview)
        canvas.configure(yscrollcommand=v_sb.set, xscrollcommand=h_sb.set)
        h_sb.pack(side='bottom', fill='x')
        v_sb.pack(side='right',  fill='y')
        canvas.pack(side='left', fill='both', expand=True)
        self._notif_canvas = canvas

        self._notif_inner = ttk.Frame(canvas)
        self._notif_inner_id = canvas.create_window((0, 0), window=self._notif_inner, anchor='nw')
        self._notif_inner.bind('<Configure>',
            lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        # no width-binding: let inner frame keep its natural width so h-scroll activates

        # Header — compact two-row layout
        _HF = ('Segoe UI', 8, 'bold')
        _ws = [200, 52, 52, 65, 65, 70, 58, 65, 58, 32]
        for col, w in enumerate(_ws):
            self._notif_inner.columnconfigure(col, minsize=w)

        # Row 0 — group labels
        tk.Label(self._notif_inner, text='Email Address', font=_HF,
                 anchor='center').grid(row=0, column=0, rowspan=2, padx=2, pady=2, sticky='ew')
        tk.Label(self._notif_inner, text='Scheduler', font=_HF,
                 anchor='center', fg='#0055aa').grid(
            row=0, column=1, columnspan=4, padx=2, pady=(2, 0), sticky='ew')
        tk.Label(self._notif_inner, text='Dwnld', font=_HF,
                 anchor='center').grid(row=0, column=5, rowspan=2, padx=2, pady=2, sticky='ew')
        tk.Label(self._notif_inner, text='Last\nFile', font=_HF,
                 anchor='center').grid(row=0, column=6, rowspan=2, padx=2, pady=2, sticky='ew')
        tk.Label(self._notif_inner, text='Checking', font=_HF,
                 anchor='center', fg='#0055aa').grid(
            row=0, column=7, columnspan=2, padx=2, pady=(2, 0), sticky='ew')

        # Row 1 — sub-headers
        for col, txt in zip([1, 2, 3, 4], ['Start', 'Stop', 'Inactive', 'Repeat\n(min)']):
            tk.Label(self._notif_inner, text=txt, font=_HF,
                     anchor='center').grid(row=1, column=col, padx=2, pady=(0, 2), sticky='ew')
        for col, txt in zip([7, 8], ['Mount\nAlert', 'Ping\nAlert']):
            tk.Label(self._notif_inner, text=txt, font=_HF,
                     anchor='center').grid(row=1, column=col, padx=2, pady=(0, 2), sticky='ew')

        ttk.Separator(self._notif_inner, orient='horizontal').grid(
            row=2, column=0, columnspan=10, sticky='ew', pady=1)

        # Existing recipients
        for rec in cfg.get('recipients', []):
            self._add_notif_row(rec)

        # Add-row bar
        add_frame = ttk.Frame(parent)
        add_frame.pack(fill='x', padx=20, pady=(2, 8))
        self.email_entry = ttk.Entry(add_frame, width=38)
        self.email_entry.pack(side='left', padx=(0, 6))
        self.email_entry.bind('<Return>', lambda e: self._add_email())
        ttk.Button(add_frame, text="Add Recipient", command=self._add_email).pack(side='left', padx=3)

        # Buttons
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill='x', padx=20, pady=6)
        ttk.Button(btn_frame, text="Configure SMTP...", command=self._smtp_dialog).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Send Test Email", command=self._send_test_email).pack(side='left', padx=5)
        smtp_cfg = load_smtp()
        smtp_status = f"SMTP: {smtp_cfg['host']}:{smtp_cfg['port']} ({smtp_cfg['user']})" if smtp_cfg else "SMTP: not configured"
        self.smtp_status_label = ttk.Label(btn_frame, text=smtp_status, foreground='#555555')
        self.smtp_status_label.pack(side='left', padx=20)

    def _add_notif_row(self, rec=None):
        row_idx = len(self._notif_rows) + 3  # +3 for 2 header rows + separator
        email_var        = tk.StringVar(value=rec.get('email', '') if rec else '')
        on_start_var     = tk.BooleanVar(value=rec.get('on_start', True) if rec else True)
        on_stop_var      = tk.BooleanVar(value=rec.get('on_stop', True) if rec else True)
        on_inactive_var  = tk.BooleanVar(value=rec.get('on_inactive', True) if rec else True)
        repeat_min_var   = tk.StringVar(value=str(rec.get('repeat_minutes', 30) if rec else 30))
        on_download_var      = tk.BooleanVar(value=rec.get('on_download', False) if rec else False)
        on_last_file_var     = tk.BooleanVar(value=rec.get('on_last_file', False) if rec else False)
        on_mount_alert_var   = tk.BooleanVar(value=rec.get('on_mount_alert', True) if rec else True)
        on_ping_alert_var    = tk.BooleanVar(value=rec.get('on_ping_alert', True) if rec else True)

        row_data = {'email': email_var, 'on_start': on_start_var, 'on_stop': on_stop_var,
                    'on_inactive': on_inactive_var, 'repeat_minutes': repeat_min_var,
                    'on_download': on_download_var, 'on_last_file': on_last_file_var,
                    'on_mount_alert': on_mount_alert_var,
                    'on_ping_alert': on_ping_alert_var, 'widgets': []}

        e = ttk.Entry(self._notif_inner, textvariable=email_var, width=26)
        e.grid(row=row_idx, column=0, padx=2, pady=1, sticky='w')
        e.bind('<FocusOut>', lambda ev: self._save_notifications())
        row_data['widgets'].append(e)

        for col, var in enumerate([on_start_var, on_stop_var, on_inactive_var], start=1):
            cb = ttk.Checkbutton(self._notif_inner, variable=var, command=self._save_notifications)
            cb.grid(row=row_idx, column=col, padx=2, pady=1)
            row_data['widgets'].append(cb)

        # Repeat (min) spinbox — col 4
        sb = ttk.Spinbox(self._notif_inner, textvariable=repeat_min_var,
                         from_=0, to=9999, width=4, command=self._save_notifications)
        sb.grid(row=row_idx, column=4, padx=2, pady=1)
        sb.bind('<FocusOut>', lambda ev: self._save_notifications())
        row_data['widgets'].append(sb)

        for col, var in [(5, on_download_var), (6, on_last_file_var),
                         (7, on_mount_alert_var), (8, on_ping_alert_var)]:
            cb = ttk.Checkbutton(self._notif_inner, variable=var, command=self._save_notifications)
            cb.grid(row=row_idx, column=col, padx=2, pady=1)
            row_data['widgets'].append(cb)

        def remove(rd=row_data):
            for w in rd['widgets'] + [rd.get('del_btn')]:
                if w: w.destroy()
            self._notif_rows.remove(rd)
            self._save_notifications()

        del_btn = ttk.Button(self._notif_inner, text="✕", width=2, command=remove)
        del_btn.grid(row=row_idx, column=9, padx=2, pady=1)
        row_data['del_btn'] = del_btn
        row_data['widgets'].append(del_btn)

        self._notif_rows.append(row_data)

    def _add_email(self):
        email = self.email_entry.get().strip()
        if email and '@' in email:
            self._add_notif_row({'email': email, 'on_start': True, 'on_stop': True,
                                 'on_inactive': True, 'repeat_minutes': 30,
                                 'on_download': False, 'on_last_file': False,
                                 'on_mount_alert': True, 'on_ping_alert': True})
            self.email_entry.delete(0, tk.END)
            self._save_notifications()

    def _remove_email(self):
        pass  # rows have individual ✕ buttons

    def _save_notifications(self):
        recipients = []
        for r in self._notif_rows:
            email = r['email'].get().strip()
            if not email:
                continue
            try:
                repeat_min = max(0, int(r['repeat_minutes'].get()))
            except (ValueError, KeyError):
                repeat_min = 0
            recipients.append({
                'email': email,
                'on_start': r['on_start'].get(),
                'on_stop': r['on_stop'].get(),
                'on_inactive': r['on_inactive'].get(),
                'repeat_minutes': repeat_min,
                'on_download': r['on_download'].get(),
                'on_last_file': r['on_last_file'].get(),
                'on_mount_alert': r['on_mount_alert'].get(),
                'on_ping_alert': r['on_ping_alert'].get(),
            })
        save_notifications(self.notif_enabled.get(), recipients)

    def _smtp_dialog(self):
        from notifier import load_smtp, save_smtp
        smtp = load_smtp() or {}
        win = tk.Toplevel(self.root)
        win.title("SMTP Configuration")
        win.geometry("420x320")
        win.resizable(False, False)

        fields = [
            ('host',  'SMTP Host',         smtp.get('host', '')),
            ('port',  'Port',              str(smtp.get('port', 587))),
            ('user',  'Username',          smtp.get('user', '')),
            ('password', 'Password',       ''),
            ('from',  'From Address',      smtp.get('from', smtp.get('user', ''))),
        ]
        entries = {}
        for i, (key, label, val) in enumerate(fields):
            ttk.Label(win, text=label + ':').grid(row=i, column=0, sticky='w', padx=20, pady=8)
            e = ttk.Entry(win, width=35, show='*' if key == 'password' else '')
            e.insert(0, val)
            e.grid(row=i, column=1, padx=10, pady=8)
            entries[key] = e

        tls_var = tk.BooleanVar(value=smtp.get('tls', True))
        ttk.Checkbutton(win, text="Use TLS (STARTTLS)", variable=tls_var).grid(
            row=len(fields), column=0, columnspan=2, pady=6)

        def save():
            pwd = entries['password'].get().strip() or smtp.get('password', '')
            if not pwd:
                messagebox.showerror("Error", "Password is required", parent=win)
                return
            save_smtp(
                host=entries['host'].get().strip(),
                port=entries['port'].get().strip(),
                user=entries['user'].get().strip(),
                password=pwd,
                from_addr=entries['from'].get().strip(),
                use_tls=tls_var.get(),
            )
            cfg = load_smtp()
            self.smtp_status_label.config(
                text=f"SMTP: {cfg['host']}:{cfg['port']} ({cfg['user']})")
            win.destroy()

        ttk.Button(win, text="Save", command=save, style="Accent.TButton").grid(
            row=len(fields)+1, column=0, columnspan=2, pady=15)

    def _send_test_email(self):
        self._save_notifications()
        ok, err = send_notification("Test", "This is a test notification from DGnet FTP Monitor.")
        if ok:
            messagebox.showinfo("Test Email", "Test email sent successfully.")
        else:
            messagebox.showerror("Test Email Failed", err or "Unknown error")

    def _show_missing_details(self, event=None):
        sel = self.summary_tree.selection()
        self.missing_text.delete(1.0, tk.END)
        if not sel:
            self.missing_text.insert(tk.END, "Click a station above to view missing files...")
            return
        item = self.summary_tree.item(sel[0])
        values = item['values']
        group = f"{values[0]} | {values[1]} | {values[2]}"
        missing_files = self._missing_by_iid.get(sel[0], [])
        if not missing_files:
            days = self.days_var.get()
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            now = datetime.now(timezone.utc)
            range_str = f"{cutoff.strftime('%Y-%m-%d %H:%M')} – {now.strftime('%Y-%m-%d %H:%M')} UTC  (last {days} day{'s' if days != 1 else ''})"
            self.missing_text.insert(tk.END, f"PERFECT! NO MISSING FILES\n\n{group}\n\nPeriod: {range_str}\n\nAll files present and correct!")
            return
        days = self.days_var.get()
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        now = datetime.now(timezone.utc)
        range_str = f"{cutoff.strftime('%Y-%m-%d %H:%M')} – {now.strftime('%Y-%m-%d %H:%M')} UTC  (last {days} day{'s' if days != 1 else ''})"
        self.missing_text.insert(tk.END, f"MISSING FILES FOR:\n{group}\n\nPeriod: {range_str}\n\n")
        for f in sorted(missing_files):
            self.missing_text.insert(tk.END, f"{f}\n")
        self.missing_text.insert(tk.END, f"\nTOTAL: {len(missing_files)} files missing")

    def _refresh_summary(self):
        self.notebook.select(1)
        for i in self.summary_tree.get_children():
            self.summary_tree.delete(i)
        self.missing_text.delete(1.0, tk.END)
        self.missing_text.insert(tk.END, "Building 100% accurate summary...")

        if not self.full_log: 
            self.status_var.set("Run SCAN first")
            return

        days = self.days_var.get()
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=days)
        delay_cutoff = now_utc - timedelta(minutes=self.delay_minutes.get())
        groups = {}

        target_log = self.summary_filter.get()
        if target_log == "All Stations":
            target_log = None

        for site_items in self.full_log.log.values():
            for item in site_items:
                site = item['site_obj']
                if target_log and site.name != target_log:
                    continue
                station = getattr(site, 'station_code', extract_station_name(item['file']))
                rate_key = f"{site.rate} {'[ExtClk]' if site.external_clock else ''}".strip()
                group_key = f"{site.network} | {station} | {site.name} | {rate_key}"
                if group_key not in groups:
                    groups[group_key] = {
                        'network': site.network,
                        'station': station,
                        'site': f"{site.name} | {rate_key}",
                        'last_dt': None, 'last_file': '', 'missing': [],
                        'last_exp_date': '', 'last_exp_ok': None,
                    }
                
                file_dt = item.get('file_dt')
                if not file_dt and item['local'] == 'yes' and os.path.exists(item['local_path']):
                    try:
                        mtime = os.path.getmtime(item['local_path'])
                        file_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
                        item['file_dt'] = file_dt
                    except OSError:
                        pass

                # For cutoff comparison use available_dt (when file becomes downloadable)
                # falling back to file_dt (local mtime) or parsed expected date
                item_dt = file_dt or item.get('available_dt')
                if not item_dt:
                    try:
                        date_str = item['date']
                        fmt = "%Y-%m-%d %H:%M" if ' ' in date_str else "%Y-%m-%d"
                        item_dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                    except (ValueError, KeyError):
                        pass

                if file_dt and (groups[group_key]['last_dt'] is None or file_dt > groups[group_key]['last_dt']):
                    groups[group_key]['last_dt'] = file_dt
                    groups[group_key]['last_file'] = item['file']

                if item['status'] in ('missing locally', 'missing remotely', 'size mismatch') and not item.get('is_current_utc', False):
                    if item_dt is None or item_dt >= cutoff:
                        groups[group_key]['missing'].append(item['file'])

                # Track the most recent completed (non-future, non-current) file status
                if not item.get('future') and not item.get('is_current_utc'):
                    date_str = item.get('date', '')
                    grp = groups[group_key]
                    if date_str > grp['last_exp_date']:
                        grp['last_exp_date'] = date_str
                        # A file is "pending" if it is within the scheduler delay window
                        # (the scheduler hasn't had a chance to download it yet)
                        pending = item_dt is not None and item_dt >= delay_cutoff
                        grp['last_exp_ok'] = (
                            item['status'] == 'ok' or
                            (item['status'] in ('missing locally', 'missing remotely') and pending)
                        )

        self._missing_by_iid.clear()
        for group, data in sorted(groups.items()):
            last_str = data['last_dt'].strftime("%Y-%m-%d %H:%M UTC") if data['last_dt'] else "Never"
            missing_files = data['missing']
            missing_count = len(missing_files)
            last_ok = data.get('last_exp_ok')   # None = no completed files seen
            if missing_count == 0:
                tag = 'ok'
            elif last_ok:
                tag = 'warning'   # older gaps, but latest file is fine
            else:
                tag = 'missing'   # latest expected file itself is missing
            iid = self.summary_tree.insert('', 'end', values=(
                data['network'], data['station'], data['site'], last_str, data['last_file'] or "—", missing_count
            ), tags=(tag,))
            self._missing_by_iid[iid] = missing_files

        total_missing = sum(len(g['missing']) for g in groups.values())
        filter_text = f" (filtered: {target_log})" if target_log else ""
        self.status_var.set(f"Summary: {len(groups)} stations | {total_missing} missing files{filter_text}")

    def _refresh_after_download(self):
        if self.auto_refresh.get():
            self._refresh_table()
            self._refresh_summary()

    def _insert_site_items(self, site_items):
        now_utc = datetime.now(timezone.utc)

        for item in site_items:
            is_daily_next = (item['status'] == 'scheduled' and
                             getattr(item.get('site_obj'), 'frequency', '') == 'daily')
            if self.show_issues.get() and item['status'] in ['ok', 'scheduled'] and not item.get('is_current_utc') and not is_daily_next:
                continue
            if self.filter_site.get() != "All Stations" and item['site'] != self.filter_site.get():
                continue

            # Column header filters
            _so = item['site_obj']
            if self.col_filters['Network'] and getattr(_so, 'network', '') not in self.col_filters['Network']:
                continue
            if self.col_filters['Station'] and getattr(_so, 'station_code', '') not in self.col_filters['Station']:
                continue
            if self.col_filters['Log Name'] and item['site'] not in self.col_filters['Log Name']:
                continue

            view_item = copy.copy(item)
            if 'file_dt' not in view_item:
                try:
                    if ' ' in view_item['date']:
                        view_item['file_dt'] = datetime.strptime(view_item['date'], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                    else:
                        view_item['file_dt'] = datetime.strptime(view_item['date'], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    view_item['file_dt'] = None

            is_current_utc = (' ' in view_item['date']
                and view_item['date'].split()[0] == now_utc.strftime("%Y-%m-%d")
                and view_item['date'].split()[1][:2] == now_utc.strftime("%H"))
            if is_current_utc and view_item['remote'] == 'yes':
                view_item['is_current_utc'] = True
                view_item['status'] = 'new'

            tag = ('current_growing' if view_item.get('is_current_utc') else
                   'conn_failed' if view_item['status'] in ('connection failed', 'ok (offline)') else
                   'ok' if view_item['status'] == 'ok' else
                   'missing_local' if view_item['status'] == 'missing locally' else
                   'missing_remote' if view_item['status'] == 'missing remotely' else
                   'mismatch' if view_item['status'] == 'size mismatch' else
                   'scheduled')

            site_obj = view_item['site_obj']
            network = getattr(site_obj, 'network', '')
            station_name = getattr(site_obj, 'station_code', extract_station_name(view_item['file']))
            log_name = view_item['site']
            freq = getattr(site_obj, 'frequency', '').capitalize()
            local_size_str = format_size(view_item['local_size']) if view_item['local'] == 'yes' else "\u2014"
            remote_size_str = format_size(view_item['remote_size']) if view_item['remote'] == 'yes' else "\u2014"

            self.tree.insert('', 'end', values=(
                network, station_name, log_name, freq, view_item['date'], view_item['file'],
                view_item['local'], local_size_str,
                view_item['remote'], remote_size_str,
                view_item['status'],
                'CURRENT (growing)' if view_item.get('is_current_utc') else 'Future' if view_item['future'] else 'Past'
            ), tags=(tag,))

    def _show_col_filter(self, col):
        if not self.full_log:
            return
        # Collect unique values for this column
        all_values = set()
        for site_items in self.full_log.log.values():
            for item in site_items:
                so = item['site_obj']
                if col == 'Network':
                    all_values.add(getattr(so, 'network', ''))
                elif col == 'Station':
                    all_values.add(getattr(so, 'station_code', ''))
                else:
                    all_values.add(item['site'])

        popup = tk.Toplevel(self.root)
        popup.title("")
        popup.resizable(False, False)
        popup.overrideredirect(True)  # borderless, like a dropdown
        popup.grab_set()
        # Position just below the column header (at the mouse cursor)
        px = self.root.winfo_pointerx()
        py = self.root.winfo_pointery() + 5
        popup.geometry(f"+{px}+{py}")

        # Bordered frame to look like an Excel dropdown
        border = tk.Frame(popup, bd=1, relief='solid', bg='#d0d0d0')
        border.pack(fill='both', expand=True)
        tk.Label(border, text=f"  Filter: {col}", bg='#e8e8e8', anchor='w',
                 font=('Segoe UI', 9, 'bold')).pack(fill='x')
        ttk.Separator(border, orient='horizontal').pack(fill='x')

        current = self.col_filters[col]
        check_vars = {}

        all_var = tk.BooleanVar(value=not current)
        all_cb = ttk.Checkbutton(border, text="(Select All)", variable=all_var)
        all_cb.pack(anchor='w', padx=10, pady=(6, 2))

        ttk.Separator(border, orient='horizontal').pack(fill='x', padx=10, pady=2)

        for val in sorted(all_values):
            var = tk.BooleanVar(value=(val in current) if current else True)
            check_vars[val] = var
            ttk.Checkbutton(border, text=val, variable=var,
                            command=lambda: all_var.set(False)).pack(anchor='w', padx=20)

        def on_all_toggled():
            if all_var.get():
                for v in check_vars.values():
                    v.set(False)

        all_var.trace_add('write', lambda *_: on_all_toggled())

        def apply():
            selected = {v for v, var in check_vars.items() if var.get()}
            self.col_filters[col] = set() if (all_var.get() or selected == all_values) else selected
            # Update header to show filter indicator
            indicator = ' ▼' if self.col_filters[col] else ''
            self.tree.heading(col, text=f"{col}{indicator}",
                              command=lambda c=col: self._show_col_filter(c))
            popup.destroy()
            self._filter_only()

        ttk.Separator(border, orient='horizontal').pack(fill='x', padx=10, pady=2)
        btn_frame = ttk.Frame(border)
        btn_frame.pack(fill='x', pady=6, padx=10)
        ttk.Button(btn_frame, text="OK", command=apply, style="Accent.TButton").pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Cancel", command=popup.destroy).pack(side='left')

    def _on_all_remote_toggle(self):
        self.days_spin.config(state='disabled' if self.all_remote_var.get() else 'normal')

    def _scan_and_download(self, auto=False):
        for i in self.tree.get_children(): self.tree.delete(i)
        self.scan_btn.config(state='disabled')

        def on_site_done(site_name, items):
            self.root.after(0, lambda: self._insert_site_items(items))

        def on_file(site_name, fname):
            self.root.after(0, lambda: self.status_var.set(f"[{site_name}] {fname}"))

        def task():
            if self.all_remote_var.get():
                self.root.after(0, lambda: self.status_var.set("Scanning ALL remote files (this may take a while)..."))
                log = self.manager.scan_all_remote(
                    progress_cb=lambda msg: self.root.after(0, lambda: self.status_var.set(msg)),
                    site_cb=on_site_done,
                    file_cb=on_file)
            else:
                self.root.after(0, lambda: self.status_var.set("Scanning Greek network..."))
                days = self.days_var.get()
                log = self.manager.scan_all(
                    days,
                    progress_cb=lambda msg: self.root.after(0, lambda: self.status_var.set(msg)),
                    site_cb=on_site_done,
                    file_cb=on_file)
            self.full_log = log
            self.root.after(0, lambda: (
                self.scan_btn.config(state='normal'),
                self.manager.auto_download_completed(log, self.delay_minutes.get()) if auto else None,
                self._refresh_summary(),
                self._refresh_sites(),
                self.root.after(0, lambda: self.status_var.set("Scan complete – v9.999.9.7"))
            ))

        threading.Thread(target=task, daemon=True).start()

    def _filter_only(self):
        if not self.full_log: return
        for i in self.tree.get_children(): self.tree.delete(i)
        for site_items in self.full_log.log.values():
            self._insert_site_items(site_items)

    def _download(self):
        if not self.full_log: return
        items = [item for sl in self.full_log.log.values() for item in sl
                 if item['status'] in ['missing locally', 'size mismatch'] and not item.get('is_current_utc')]
        if not items:
            messagebox.showinfo("Done", "No completed files to download")
            return

        self.dl_btn.config(state='disabled')
        self.progress['maximum'] = len(items)
        self.progress['value'] = 0

        def dl():
            self.manager.download_missing(items,
                lambda msg: self.root.after(0, lambda: (
                    self.progress.config(value=self.progress['value'] + 1),
                    self.status_var.set(msg)
                )))
            self.root.after(0, lambda: (
                self.dl_btn.config(state='normal'),
                self._refresh_after_download(),
                messagebox.showinfo("Success", f"Downloaded {len(items)} files!")
            ))

        threading.Thread(target=dl, daemon=True).start()

    def _launch_watchdog(self):
        try:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scheduler.pid')) as f:
                sched_pid = f.read().strip()
            python = sys.executable
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'watchdog.py')
            if os.name == 'nt':
                flags = subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
                try:
                    subprocess.Popen([python, script, sched_pid], creationflags=flags, close_fds=True)
                except OSError:
                    subprocess.Popen([python, script, sched_pid],
                                     creationflags=flags & ~subprocess.CREATE_BREAKAWAY_FROM_JOB, close_fds=True)
            else:
                devnull = open(os.devnull, 'rb')
                subprocess.Popen([python, script, sched_pid], start_new_session=True, close_fds=True,
                                 stdin=devnull, stdout=devnull, stderr=devnull)
            logger.info("Launched watchdog for scheduler PID %s", sched_pid)
        except Exception as e:
            logger.warning("Could not launch watchdog: %s", e)

    def _toggle_scheduler(self):
        if not self.scheduler_running:
            # Write control file and launch service process
            delay = self.delay_minutes.get()
            days = self.days_var.get()
            write_control(True, delay, days)

            newly_launched = False
            if not is_service_running():
                # Launch headless process detached from this GUI
                python = sys.executable
                script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main.py')
                if os.name == 'nt':
                    flags = (subprocess.CREATE_NO_WINDOW |
                             subprocess.DETACHED_PROCESS |
                             subprocess.CREATE_NEW_PROCESS_GROUP |
                             subprocess.CREATE_BREAKAWAY_FROM_JOB)
                    try:
                        subprocess.Popen([python, script, '--headless'],
                                         creationflags=flags, close_fds=True)
                    except OSError:
                        subprocess.Popen([python, script, '--headless'],
                                         creationflags=flags & ~subprocess.CREATE_BREAKAWAY_FROM_JOB,
                                         close_fds=True)
                else:
                    devnull = open(os.devnull, 'rb')
                    subprocess.Popen([python, script, '--headless'],
                                     start_new_session=True, close_fds=True,
                                     stdin=devnull, stdout=devnull, stderr=devnull)
                logger.info("Launched scheduler service process")
                newly_launched = True
                # Launch watchdog in background after brief delay (scheduler writes PID file first)
                self.root.after(3000, self._launch_watchdog)

            self.scheduler_running = True
            self.scheduler_btn.config(text="STOP SCHEDULER")
            self.led.delete("dot")
            self.led.create_oval(4, 4, 14, 14, fill="lime", outline="green", width=3, tags="dot")
            self._schedule_next_run()
            self.scheduler_thread = threading.Thread(
                target=self._scheduler_loop, args=(newly_launched,), daemon=True)
            self.scheduler_thread.start()
        else:
            # Signal service to stop via control file
            write_control(False)
            self.scheduler_running = False
            self.scheduler_btn.config(text="START SCHEDULER")
            self.led.delete("dot")
            self.led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
            self._set_download_idle()
            self.scheduler_var.set("Scheduler stopped")

    def _schedule_next_run(self):
        now = datetime.now()
        delay = self.delay_minutes.get()
        candidate = now.replace(minute=delay, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(hours=1)
        self.next_run_time = candidate
        remaining = int((self.next_run_time - now).total_seconds())
        self.scheduler_var.set(f"Next run: {self.next_run_time.strftime('%H:%M')} (in {self._format_countdown(remaining)})")

    def _scheduler_loop(self, newly_launched=False):
        """Monitor the external service process and update the GUI countdown."""
        # When we just launched the service, give it up to 5 s to write its PID file
        # before treating a missing PID as a crash.
        startup_deadline = time.time() + 5 if newly_launched else 0

        while self.scheduler_running:
            # Check if service is still alive
            if not is_service_running():
                ctrl = read_control()
                if ctrl and ctrl.get('running', False):
                    if time.time() < startup_deadline:
                        # Still within startup grace period – wait and retry
                        time.sleep(0.5)
                        continue
                    # Service died unexpectedly, restart it
                    logger.warning("Scheduler service not running, restarting...")
                    python = sys.executable
                    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main.py')
                    if os.name == 'nt':
                        flags = (subprocess.CREATE_NO_WINDOW |
                                 subprocess.DETACHED_PROCESS |
                                 subprocess.CREATE_NEW_PROCESS_GROUP |
                                 subprocess.CREATE_BREAKAWAY_FROM_JOB)
                        try:
                            subprocess.Popen([python, script, '--headless'],
                                             creationflags=flags, close_fds=True)
                        except OSError:
                            subprocess.Popen([python, script, '--headless'],
                                             creationflags=flags & ~subprocess.CREATE_BREAKAWAY_FROM_JOB,
                                             close_fds=True)
                    else:
                        devnull = open(os.devnull, 'rb')
                        subprocess.Popen([python, script, '--headless'],
                                         start_new_session=True, close_fds=True,
                                         stdin=devnull, stdout=devnull, stderr=devnull)
                    startup_deadline = time.time() + 5  # grace period for the restarted process
                else:
                    # Service stopped itself
                    self.root.after(0, self._on_service_stopped)
                    return

            # Update control file with current GUI settings
            write_control(True, self.delay_minutes.get(), self.days_var.get())

            self._poll_log_for_download()

            now = datetime.now()
            self._schedule_next_run()
            remaining = int((self.next_run_time - now).total_seconds())
            if remaining > 0:
                svc_label = "SERVICE RUNNING"
                self.root.after(0, lambda r=remaining: self.scheduler_var.set(
                    f"{svc_label} | Next run: {self.next_run_time.strftime('%H:%M')} (in {self._format_countdown(r)})"))
            time.sleep(5)

    def _on_service_stopped(self):
        """Called when the external service stops."""
        self.scheduler_running = False
        self.scheduler_btn.config(text="START SCHEDULER")
        self.led.delete("dot")
        self.led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
        self._set_download_idle()
        self.scheduler_var.set("Service stopped")

    def _init_log_pos(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scheduler.log')
        try:
            with open(log_path, 'r') as f:
                f.seek(0, 2)
                self._log_pos = f.tell()
        except OSError:
            self._log_pos = 0

    def _poll_log_for_download(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scheduler.log')
        try:
            with open(log_path, 'r') as f:
                f.seek(self._log_pos)
                lines = f.readlines()
                self._log_pos = f.tell()
        except OSError:
            return
        for line in lines:
            m = re.search(r'Downloading (.+?) \((\d+)/(\d+)\)', line)
            if m:
                fname, n, total = m.group(1), int(m.group(2)), int(m.group(3))
                self.root.after(0, lambda f=fname, n=n, t=total: self._set_downloading(f, n, t))
            elif 'Files Downloaded' in line or re.search(r'Scan complete: \d+', line):
                self.root.after(0, self._set_download_idle)

    def _set_downloading(self, fname, n, total):
        self.dl_led.delete("dot")
        self.dl_led.create_oval(4, 4, 14, 14, fill="orange", outline="darkorange", width=2, tags="dot")
        self.progress['maximum'] = total
        self.progress['value'] = n
        self.status_var.set(f"[SCHEDULER] Downloading {fname} ({n}/{total})")

    def _set_download_idle(self):
        self.dl_led.delete("dot")
        self.dl_led.create_oval(4, 4, 14, 14, fill="#d0d0d0", tags="dot")
        self.progress['value'] = 0

    def _format_countdown(self, seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return f"{h:02d}h {m:02d}m {s:02d}s"

    def _on_tree_select(self, event):
        sel = self.tree_sites.selection()
        if sel:
            item = self.tree_sites.item(sel[-1])
            if item['values']:
                self.selected_log_name = item['values'][0]
                if self.summary_filter.get() == self.selected_log_name:
                    self._refresh_summary()

    def _on_tree_click(self, event):
        iid = self.tree_sites.identify_row(event.y)
        col = self.tree_sites.identify_column(event.x)
        if not iid or col != '#2':
            return  # only toggle when clicking the checkbox column
        values = self.tree_sites.item(iid, 'values')
        if not values or not values[0]:
            return  # network or station group node — no checkbox
        site_name = values[0]
        site = next((s for s in self.manager.sites if s.name == site_name), None)
        if site is None:
            return
        site.enabled = not getattr(site, 'enabled', True)
        self.manager._save()
        mark = '☑' if site.enabled else '☐'
        self.tree_sites.set(iid, 'chk', mark)
        self.tree_sites.item(iid, tags=('leaf_enabled',) if site.enabled else ('leaf_disabled',))

    def _refresh_sites(self):
        def build_tree():
            networks = {}
            for site in self.manager.sites:
                net = site.network or "Unknown"
                station = site.station_code or site.name
                networks.setdefault(net, {}).setdefault(station, []).append(site)
            return networks

        def update_ui(networks):
            for item in self.tree_sites.get_children():
                self.tree_sites.delete(item)

            for net, stations in sorted(networks.items()):
                net_id = self.tree_sites.insert('', 'end', text=f" {net.upper()}", open=True)
                for station_name, sites in sorted(stations.items()):
                    station_id = self.tree_sites.insert(net_id, 'end', text=f"  {station_name}", open=True)
                    for site in sites:
                        rate_key = f"{site.rate} {'[ExtClk]' if site.external_clock else ''}".strip()
                        enabled = getattr(site, 'enabled', True)
                        mark = '☑' if enabled else '☐'
                        tag = ('leaf_enabled',) if enabled else ('leaf_disabled',)
                        self.tree_sites.insert(station_id, 'end',
                            text=f"  {site.name} - {rate_key}",
                            values=(site.name, mark), tags=tag)

            station_list = ["All Stations"] + sorted([s.name for s in self.manager.sites])
            self.summary_combo['values'] = station_list
            if self.summary_filter.get() not in station_list:
                self.summary_filter.set("All Stations")
            self.combo['values'] = station_list
            if self.manager.sites:
                self.combo.current(0)

        networks = build_tree()
        update_ui(networks)

    def _edit_dialog(self, site=None, idx=None):
        win = tk.Toplevel(self.root)
        win.title("Add Station" if not site else "Edit Station")
        win.geometry("720x1100")

        fields = [
            ('network', 'Network (e.g. NOA)'),
            ('station_code', 'Station Name (4-letter)'),
            ('name', 'Log Name (e.g. NOA1)'),
            ('rate', 'Rate (1s/30s)'),
            ('format', 'Format'),
            ('host', 'Host'), ('protocol', 'Protocol'), ('user', 'User'), ('password', 'Password'),
            ('path', 'Path'), ('pattern', 'Pattern'), ('frequency', 'Frequency'), ('output_dir', 'Local Folder')
        ]
        ents = {}
        ext_clk = tk.BooleanVar(value=site.external_clock if site else False)
        letter = tk.BooleanVar(value=site.use_letter_hour if site else False)
        format_var = tk.StringVar(value=getattr(site, 'format', 'Topcon'))

        for i, (key, label) in enumerate(fields):
            if key == 'format':
                ttk.Label(win, text=label + ":").grid(row=i, column=0, sticky='w', padx=20, pady=8)
                combo = ttk.Combobox(win, textvariable=format_var, values=['Topcon', 'Trimble', 'South'], state='readonly', width=53)
                combo.grid(row=i, column=1, padx=20, pady=8)
                ents[key] = format_var
            else:
                ttk.Label(win, text=label + ":").grid(row=i, column=0, sticky='w', padx=20, pady=8)
                e = ttk.Entry(win, width=55)
                if site and key in site.__dict__:
                    e.insert(0, getattr(site, key, ''))
                e.grid(row=i, column=1, padx=20, pady=8)
                ents[key] = e

        ttk.Checkbutton(win, text="External Clock", variable=ext_clk).grid(row=len(fields), column=0, columnspan=2, pady=10)
        ttk.Checkbutton(win, text="Use letter hour (a-x)", variable=letter).grid(row=len(fields)+1, column=0, columnspan=2, pady=10)
        ping_check = tk.BooleanVar(value=getattr(site, 'ping_check', False) if site else False)
        ttk.Checkbutton(win, text="Enable ping check", variable=ping_check).grid(row=len(fields)+2, column=0, columnspan=2, pady=10)
        enabled_var = tk.BooleanVar(value=getattr(site, 'enabled', True) if site else True)
        ttk.Checkbutton(win, text="Include in scheduler (enabled)", variable=enabled_var).grid(row=len(fields)+3, column=0, columnspan=2, pady=10)

        # Pattern reference
        ref_frame = ttk.LabelFrame(win, text=" Pattern Reference (strftime codes) ")
        ref_frame.grid(row=len(fields)+4, column=0, columnspan=2, padx=20, pady=10, sticky='ew')
        ref_text = (
            "%Y = 4-digit year (2026)    %y = 2-digit year (26)    %m = month (03)    %d = day (20)\n"
            "%H = hour 00-23             %M = minute 00-59         %j = day of year (079)\n"
            "Path and Pattern both support these codes.  Example path: /%Y%m/%d\n"
            "Example pattern: STATION%m%d%H.tps   (with letter-hour: %H replaced by a-x)"
        )
        ttk.Label(ref_frame, text=ref_text, font=('Consolas', 9), foreground='#555555',
                  justify='left').pack(padx=10, pady=8)

        def save():
            data = {k: (v.get() if isinstance(v, tk.StringVar) else v.get().strip()) for k,v in ents.items()}
            data['external_clock'] = ext_clk.get()
            data['use_letter_hour'] = letter.get()
            data['ping_check'] = ping_check.get()
            data['enabled'] = enabled_var.get()
            try:
                if site:
                    self.manager.edit_site(idx, **data)
                else:
                    self.manager.add_site(**data)
                self._refresh_sites()
                if self.ping_enabled_var and self.ping_enabled_var.get():
                    self._refresh_ping_tree()
                win.destroy()
            except Exception as e:
                messagebox.showerror("Error", str(e))
        ttk.Button(win, text="Save Station", command=save).grid(row=len(fields)+5, column=0, columnspan=2, pady=20)

    def _add_site(self): self._edit_dialog()
    def _edit_site(self):
        sel = self.tree_sites.selection()
        if not sel: return
        item = self.tree_sites.item(sel[-1])
        if item['values']:
            log_name = item['values'][0]
            for idx, site in enumerate(self.manager.sites):
                if site.name == log_name:
                    self._edit_dialog(site, idx)
                    break

    def _delete_site(self):
        sel = self.tree_sites.selection()
        if not sel: return
        item = self.tree_sites.item(sel[-1])
        if not item['values']: return
        log_name = item['values'][0]
        if messagebox.askyesno("Delete", f"Delete station {log_name} permanently?"):
            for idx, site in enumerate(self.manager.sites):
                if site.name == log_name:
                    self.manager.delete_site(idx)
                    self._refresh_sites()
                    self._refresh_table()
                    break

    def _refresh_table(self):
        self.notebook.select(0)
        self._scan_and_download(auto=False)

    def _update_status_display(self, *args):
        text = self.status_var.get()
        self.status_text.config(state='normal')
        self.status_text.delete(1.0, tk.END)
        self.status_text.insert(tk.END, text)
        self.status_text.config(state='disabled')

    # ── Checking tab ───────────────────────────────────────────────────────

    def _build_checkings_tab(self, parent):
        watcher_frame = ttk.LabelFrame(parent, text=" Checking Watcher Service ")
        watcher_frame.pack(fill='x', padx=15, pady=10)

        wctrl = ttk.Frame(watcher_frame)
        wctrl.pack(fill='x', padx=10, pady=8)

        self.mount_watcher_btn = ttk.Button(wctrl, text="START WATCHER",
                                            command=self._toggle_mount_watcher)
        self.mount_watcher_btn.pack(side=tk.LEFT, padx=5)
        self.mount_led = tk.Canvas(wctrl, width=18, height=18, bg="#f0f0f0", highlightthickness=0)
        self.mount_led.pack(side=tk.LEFT, padx=6)
        self.mount_led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
        ttk.Label(wctrl, text="Check interval:").pack(side=tk.LEFT, padx=(20, 5))
        self.mount_interval = tk.IntVar(value=1)
        ttk.Spinbox(wctrl, from_=1, to=1440, textvariable=self.mount_interval,
                    width=6).pack(side=tk.LEFT)
        ttk.Label(wctrl, text="minutes").pack(side=tk.LEFT, padx=5)

        mp_frame = ttk.LabelFrame(parent, text=" Mount Points ")
        mp_frame.pack(fill='both', expand=True, padx=15, pady=(0, 10))

        tree_frame = ttk.Frame(mp_frame)
        tree_frame.pack(fill='both', expand=True, padx=10, pady=(10, 5))
        vsb = ttk.Scrollbar(tree_frame, orient='vertical')
        cols = ('path', 'name', 'status', 'last_checked')
        self.mount_tree = ttk.Treeview(tree_frame, columns=cols, show='headings',
                                       yscrollcommand=vsb.set)
        vsb.config(command=self.mount_tree.yview)
        vsb.pack(side='right', fill='y')
        self.mount_tree.pack(fill='both', expand=True)

        self.mount_tree.heading('path', text='Mount Path')
        self.mount_tree.heading('name', text='Name')
        self.mount_tree.heading('status', text='Status')
        self.mount_tree.heading('last_checked', text='Last Checked')
        self.mount_tree.column('path', width=280, anchor='w')
        self.mount_tree.column('name', width=160, anchor='w')
        self.mount_tree.column('status', width=130, anchor='center')
        self.mount_tree.column('last_checked', width=100, anchor='center')
        self.mount_tree.tag_configure('mounted', foreground='darkgreen', background='#ccffcc')
        self.mount_tree.tag_configure('not_mounted', foreground='red', background='#ffcccc',
                                      font=('Segoe UI', 9, 'bold'))
        self.mount_tree.tag_configure('error', foreground='darkorange', background='#fff3cc')

        btn_frame = ttk.Frame(mp_frame)
        btn_frame.pack(pady=8)
        ttk.Button(btn_frame, text="Add Mountpoint",
                   command=self._add_mountpoint_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Remove",
                   command=self._remove_mountpoint).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Check Now", style="Accent.TButton",
                   command=self._refresh_mount_tree).pack(side=tk.LEFT, padx=5)

        # Station Reachability section
        ping_frame = ttk.LabelFrame(parent, text=" Station Reachability ")
        ping_frame.pack(fill='both', expand=True, padx=15, pady=(0, 10))

        ping_ctrl = ttk.Frame(ping_frame)
        ping_ctrl.pack(fill='x', padx=10, pady=8)
        ping_cfg = self._load_mount_config()
        self.ping_enabled_var = tk.BooleanVar(value=ping_cfg.get('ping_enabled', False))
        ttk.Checkbutton(ping_ctrl, text="Enable", variable=self.ping_enabled_var,
                        command=self._on_ping_toggle).pack(side=tk.LEFT, padx=5)
        ttk.Button(ping_ctrl, text="Check Now", style="Accent.TButton",
                   command=self._ping_check_now).pack(side=tk.LEFT, padx=(20, 5))

        ping_tree_frame = ttk.Frame(ping_frame)
        ping_tree_frame.pack(fill='both', expand=True, padx=10, pady=(0, 5))
        vsb2 = ttk.Scrollbar(ping_tree_frame, orient='vertical')
        ping_cols = ('network', 'station', 'host', 'status', 'last_checked')
        self.ping_tree = ttk.Treeview(ping_tree_frame, columns=ping_cols, show='headings',
                                      yscrollcommand=vsb2.set)
        vsb2.config(command=self.ping_tree.yview)
        vsb2.pack(side='right', fill='y')
        self.ping_tree.pack(fill='both', expand=True)
        self.ping_tree.heading('network', text='Network')
        self.ping_tree.heading('station', text='Station')
        self.ping_tree.heading('host', text='Host')
        self.ping_tree.heading('status', text='Status')
        self.ping_tree.heading('last_checked', text='Last Checked')
        self.ping_tree.column('network', width=90, anchor='w')
        self.ping_tree.column('station', width=90, anchor='w')
        self.ping_tree.column('host', width=200, anchor='w')
        self.ping_tree.column('status', width=130, anchor='center')
        self.ping_tree.column('last_checked', width=100, anchor='center')
        self.ping_tree.tag_configure('reachable', foreground='darkgreen', background='#ccffcc')
        self.ping_tree.tag_configure('unreachable', foreground='red', background='#ffcccc',
                                     font=('Segoe UI', 9, 'bold'))

        self._refresh_mount_tree()
        self._check_mount_watcher_status()
        if self._is_mount_watcher_running():
            logger.info("Detected running checking watcher service")
        if self.ping_enabled_var.get():
            self._refresh_ping_tree()

    def _load_mount_config(self):
        cfg_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mount_watcher_config.json')
        try:
            with open(cfg_file) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {'mountpoints': [], 'interval': 60}

    def _save_mount_config(self, cfg):
        cfg_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mount_watcher_config.json')
        with open(cfg_file, 'w') as f:
            json.dump(cfg, f, indent=2)

    def _is_mount_watcher_running(self):
        # First check systemd service status
        try:
            r = subprocess.run(
                ['systemctl', '--user', 'is-active', 'dgnet-mount-watcher.service'],
                capture_output=True, text=True, timeout=5)
            if r.stdout.strip() == 'active':
                return True
        except (OSError, subprocess.TimeoutExpired):
            pass
        # Fallback: PID file check
        pid_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mount_watcher.pid')
        try:
            with open(pid_file) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            return True
        except (FileNotFoundError, ValueError, OSError):
            return False

    def _toggle_mount_watcher(self):
        if self._is_mount_watcher_running():
            try:
                subprocess.run(['systemctl', '--user', 'stop', 'dgnet-mount-watcher.service'],
                               timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                # Fallback: kill via PID file
                pid_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mount_watcher.pid')
                try:
                    with open(pid_file) as f:
                        pid = int(f.read().strip())
                    os.kill(pid, signal.SIGTERM)
                except (FileNotFoundError, ValueError, OSError):
                    pass
            self.mount_watcher_btn.config(text="START WATCHER")
            self.mount_led.delete("dot")
            self.mount_led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
        else:
            cfg = self._load_mount_config()
            cfg['interval'] = self.mount_interval.get()
            self._save_mount_config(cfg)
            try:
                subprocess.run(['systemctl', '--user', 'start', 'dgnet-mount-watcher.service'],
                               timeout=10)
                logger.info("Started dgnet-mount-watcher via systemd")
            except (OSError, subprocess.TimeoutExpired):
                # Fallback: launch directly
                python = sys.executable
                script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mount_watcher.py')
                devnull = open(os.devnull, 'rb')
                subprocess.Popen([python, script], start_new_session=True, close_fds=True,
                                 stdin=devnull, stdout=devnull, stderr=devnull)
                logger.info("Started checking watcher directly (systemd fallback)")
            self.mount_watcher_btn.config(text="STOP WATCHER")
            self.mount_led.delete("dot")
            self.mount_led.create_oval(4, 4, 14, 14, fill="lime", outline="green", width=3, tags="dot")

    def _check_mount_watcher_status(self):
        running = self._is_mount_watcher_running()
        if hasattr(self, 'mount_watcher_btn'):
            self.mount_watcher_btn.config(text="STOP WATCHER" if running else "START WATCHER")
            self.mount_led.delete("dot")
            if running:
                self.mount_led.create_oval(4, 4, 14, 14, fill="lime", outline="green", width=3, tags="dot")
            else:
                self.mount_led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
        self.root.after(5000, self._check_mount_watcher_status)

    def _refresh_mount_tree(self):
        if self.mount_tree is None:
            return
        cfg = self._load_mount_config()
        mountpoints = cfg.get('mountpoints', [])

        def check():
            now = datetime.now().strftime('%H:%M:%S')
            results = []
            for mp in mountpoints:
                path = mp['path'] if isinstance(mp, dict) else mp
                name = mp.get('name', path) if isinstance(mp, dict) else path
                try:
                    mounted = os.path.ismount(path)
                    status = 'Mounted' if mounted else 'NOT MOUNTED'
                    tag = 'mounted' if mounted else 'not_mounted'
                except Exception:
                    status = 'Error'
                    tag = 'error'
                results.append((path, name, status, now, tag))
            self.root.after(0, lambda: self._update_mount_tree(results))

        threading.Thread(target=check, daemon=True).start()
        self.root.after(30000, self._refresh_mount_tree)

    def _update_mount_tree(self, results):
        if self.mount_tree is None:
            return
        for iid in self.mount_tree.get_children():
            self.mount_tree.delete(iid)
        for path, name, status, last_checked, tag in results:
            self.mount_tree.insert('', 'end', values=(path, name, status, last_checked),
                                   tags=(tag,))

    def _get_fstab_mountpoints(self):
        """Parse /etc/fstab and return list of (mountpoint, device, fstype)."""
        entries = []
        try:
            with open('/etc/fstab') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    device, mountpoint, fstype = parts[0], parts[1], parts[2]
                    if mountpoint == 'none':
                        continue
                    entries.append((mountpoint, device, fstype))
        except OSError:
            pass
        return entries

    def _add_mountpoint_dialog(self):
        fstab = self._get_fstab_mountpoints()

        win = tk.Toplevel(self.root)
        win.title("Add Mountpoint")
        win.geometry("520x380")
        win.resizable(False, False)

        path_var = tk.StringVar()
        name_var = tk.StringVar()

        if fstab:
            ttk.Label(win, text="Select from fstab:", font=('Segoe UI', 9, 'bold')).pack(
                anchor='w', padx=15, pady=(12, 4))

            list_frame = ttk.Frame(win)
            list_frame.pack(fill='both', padx=15, pady=(0, 8))
            vsb = ttk.Scrollbar(list_frame, orient='vertical')
            lb = tk.Listbox(list_frame, height=6, font=('Consolas', 9),
                            yscrollcommand=vsb.set, activestyle='dotbox')
            vsb.config(command=lb.yview)
            vsb.pack(side='right', fill='y')
            lb.pack(fill='both', expand=True)

            for mountpoint, device, fstype in fstab:
                lb.insert('end', f"{mountpoint}   ({device}, {fstype})")

            def on_select(event=None):
                sel = lb.curselection()
                if not sel:
                    return
                mountpoint = fstab[sel[0]][0]
                path_var.set(mountpoint)
                if not name_var.get():
                    name_var.set(os.path.basename(mountpoint) or mountpoint)

            lb.bind('<<ListboxSelect>>', on_select)
            ttk.Separator(win, orient='horizontal').pack(fill='x', padx=15, pady=4)

        form = ttk.Frame(win)
        form.pack(fill='x', padx=15, pady=4)
        ttk.Label(form, text="Mount Path:").grid(row=0, column=0, sticky='w', pady=6)
        ttk.Entry(form, textvariable=path_var, width=38).grid(row=0, column=1, padx=10, pady=6)
        ttk.Label(form, text="Name (optional):").grid(row=1, column=0, sticky='w', pady=6)
        ttk.Entry(form, textvariable=name_var, width=38).grid(row=1, column=1, padx=10, pady=6)

        def save():
            path = path_var.get().strip()
            if not path:
                return
            name = name_var.get().strip() or path
            cfg = self._load_mount_config()
            cfg.setdefault('mountpoints', []).append({'path': path, 'name': name})
            self._save_mount_config(cfg)
            win.destroy()
            self._refresh_mount_tree()

        btn_frame = ttk.Frame(win)
        btn_frame.pack(pady=12)
        ttk.Button(btn_frame, text="Add", command=save,
                   style="Accent.TButton").pack(side='left', padx=8)
        ttk.Button(btn_frame, text="Cancel", command=win.destroy).pack(side='left')

    def _remove_mountpoint(self):
        if self.mount_tree is None:
            return
        sel = self.mount_tree.selection()
        if not sel:
            return
        path = self.mount_tree.item(sel[0])['values'][0]
        cfg = self._load_mount_config()
        cfg['mountpoints'] = [
            mp for mp in cfg.get('mountpoints', [])
            if (mp['path'] if isinstance(mp, dict) else mp) != path
        ]
        self._save_mount_config(cfg)
        self._refresh_mount_tree()

    def _on_ping_toggle(self):
        cfg = self._load_mount_config()
        cfg['ping_enabled'] = self.ping_enabled_var.get()
        self._save_mount_config(cfg)
        if self.ping_enabled_var.get():
            self._refresh_ping_tree()
        else:
            self._stop_ping_refresh()

    def _stop_ping_refresh(self):
        if self._ping_after_id:
            self.root.after_cancel(self._ping_after_id)
            self._ping_after_id = None
        if self.ping_tree:
            for iid in self.ping_tree.get_children():
                self.ping_tree.delete(iid)

    def _refresh_ping_tree(self):
        if not self.ping_enabled_var or not self.ping_enabled_var.get():
            return
        stations = [s for s in self.manager.sites if getattr(s, 'ping_check', False)]

        def check():
            now = datetime.now().strftime('%H:%M:%S')
            results = []
            for site in stations:
                try:
                    res = subprocess.run(
                        ['ping', '-c', '1', '-W', '2', site.host],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                    reachable = res.returncode == 0
                except Exception:
                    reachable = False
                status = 'Reachable' if reachable else 'UNREACHABLE'
                tag = 'reachable' if reachable else 'unreachable'
                results.append((site.network, site.station_code or site.name,
                                site.host, status, now, tag))
            self.root.after(0, lambda: self._update_ping_tree(results))

        threading.Thread(target=check, daemon=True).start()
        interval_ms = max(1, self.mount_interval.get()) * 60 * 1000
        self._ping_after_id = self.root.after(interval_ms, self._refresh_ping_tree)

    def _ping_check_now(self):
        if self._ping_after_id:
            self.root.after_cancel(self._ping_after_id)
            self._ping_after_id = None
        self._refresh_ping_tree()

    def _update_ping_tree(self, results):
        if self.ping_tree is None:
            return
        for iid in self.ping_tree.get_children():
            self.ping_tree.delete(iid)
        current_hosts = set()
        for network, station, host, status, last_checked, tag in results:
            self.ping_tree.insert('', 'end', values=(network, station, host, status, last_checked),
                                  tags=(tag,))
            reachable = (tag == 'reachable')
            current_hosts.add(host)
            prev = self._prev_ping_state.get(host)
            if prev is None:
                # First check — only alert if unreachable
                if not reachable:
                    threading.Thread(
                        target=notify_ping_alert, args=(station, host), kwargs={'reachable': False},
                        daemon=True).start()
            elif prev and not reachable:
                threading.Thread(
                    target=notify_ping_alert, args=(station, host), kwargs={'reachable': False},
                    daemon=True).start()
            elif not prev and reachable:
                threading.Thread(
                    target=notify_ping_alert, args=(station, host), kwargs={'reachable': True},
                    daemon=True).start()
            self._prev_ping_state[host] = reachable
        # Remove hosts no longer monitored
        for h in list(self._prev_ping_state):
            if h not in current_hosts:
                del self._prev_ping_state[h]

    # ── End Checking tab ───────────────────────────────────────────────────

    def _on_close(self):
        sched_running = self.scheduler_running and is_service_running()
        watcher_running = self._is_mount_watcher_running()
        if sched_running or watcher_running:
            self._show_exit_dialog(sched_running, watcher_running)
            return  # dialog calls destroy when OK is pressed
        self._stop_ping_refresh()
        self.root.destroy()

    def _show_exit_dialog(self, sched_running, watcher_running):
        dlg = tk.Toplevel(self.root)
        dlg.title("Services Running")
        dlg.resizable(False, False)
        dlg.grab_set()

        F  = ('Monospace', 8)
        FB = ('Monospace', 8, 'bold')
        FS = ('Segoe UI', 8)

        pad = dict(padx=12, pady=3)

        tk.Label(dlg, text="The following services continue running after you close the app.",
                 font=('Segoe UI', 9), pady=8).pack()

        ttk.Separator(dlg, orient='horizontal').pack(fill='x', padx=10)

        services = []
        if sched_running:
            services.append(("Scheduler", "dgnet-ftp-scheduler"))
        if watcher_running:
            services.append(("Checking Watcher", "dgnet-mount-watcher"))

        def _copy(cmd):
            dlg.clipboard_clear()
            dlg.clipboard_append(cmd)

        for label, svc in services:
            f = ttk.LabelFrame(dlg, text=f" {label} ", padding=6)
            f.pack(fill='x', padx=12, pady=(8, 2))
            for action, desc in [
                ("status",  "show current state and recent log"),
                ("stop",    "stop the service"),
                ("start",   "start the service"),
                ("restart", "restart the service"),
            ]:
                cmd = f"systemctl --user {action} {svc}"
                row = ttk.Frame(f)
                row.pack(fill='x', pady=1)
                tk.Label(row, text=cmd, font=F, anchor='w', fg='#003580').pack(side='left')
                tk.Label(row, text=f"  — {desc}", font=FS, fg='#555555',
                         anchor='w').pack(side='left')
                tk.Button(row, text="⧉", font=('Segoe UI', 8), relief='flat', bd=0,
                          fg='#555555', activeforeground='#003580', cursor='hand2',
                          command=lambda c=cmd: _copy(c)).pack(side='right', padx=(4, 0))

        ttk.Separator(dlg, orient='horizontal').pack(fill='x', padx=10, pady=(10, 0))
        ttk.Button(dlg, text="OK", style="Accent.TButton",
                   command=lambda: [self._stop_ping_refresh(),
                                    dlg.destroy(), self.root.destroy()]).pack(pady=8)

        dlg.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width()  - dlg.winfo_width())  // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")

    def run(self):
        self.root.mainloop()


if __name__ == '__main__':
    mgr = FTPSiteManager()
    app = FTPSiteGUI(mgr)
    app.run()
