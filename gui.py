import copy
import logging
import os
import re
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
        self.root.title("DGnet FTP Monitor - GREEK NATIONAL v9.999.9.7 - OFFICIAL FINAL - 10 Nov 2025 03:09 PM EET")
        self.root.geometry("1950x1080")
        self.root.minsize(1700, 950)
        self.days_var = tk.IntVar(value=1)
        self.all_remote_var = tk.BooleanVar(value=False)
        self.summary_days_var = tk.IntVar(value=7)
        self.show_issues = tk.BooleanVar(value=True)
        self.filter_site = tk.StringVar(value="All Stations")
        self.summary_filter = tk.StringVar(value="All Stations")
        self.auto_refresh = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="Ready")
        self.scheduler_var = tk.StringVar(value="Scheduler: Stopped")
        self.delay_minutes = tk.IntVar(value=15)
        self.full_log = None
        self.scheduler_running = False
        self.scheduler_thread = None
        self.next_run_time = None
        self.missing_text = None
        self._missing_by_iid = {}
        self._build_ui()
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
                self.days_var.set(ctrl.get('days', 1))
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
        self.tree_sites = ttk.Treeview(left_frame, show='tree', selectmode='extended')
        self.tree_sites.pack(fill='both', expand=True, padx=20, pady=10)
        self.tree_sites.bind('<<TreeviewSelect>>', self._on_tree_select)
        
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
        self.days_spin = ttk.Spinbox(row1, from_=1, to=365, textvariable=self.days_var, width=5, command=self._refresh_table)
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
        ttk.Label(row2, text="Minutes after hour:").pack(side=tk.LEFT, padx=8)
        ttk.Spinbox(row2, from_=1, to=59, textvariable=self.delay_minutes, width=5).pack(side=tk.LEFT, padx=5)

        ttk.Label(ctrl, textvariable=self.scheduler_var, foreground="#006400", font=('Segoe UI', 10, 'bold'), anchor='w').pack(fill='x', padx=15, pady=4)
        self.progress = ttk.Progressbar(ctrl, mode='determinate')
        self.progress.pack(fill='x', padx=15, pady=8)

        columns = ('Log Name','Station','Date (UTC)','File','Local','Local Size','Remote','Remote Size','Status','Type')
        tree_frame = ttk.Frame(tab1)
        tree_frame.pack(fill='both', expand=True, padx=15, pady=10)
        h_scroll = ttk.Scrollbar(tree_frame, orient='horizontal')
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings', xscrollcommand=h_scroll.set)
        h_scroll.config(command=self.tree.xview)
        h_scroll.pack(side='bottom', fill='x')
        self.tree.pack(fill='both', expand=True)

        widths = [120,110,150,450,60,90,60,90,160,100]
        for c, w in zip(columns, widths):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor='center')
        self.tree.column('File', anchor='w')
        self.tree.column('Local Size', anchor='e')
        self.tree.column('Remote Size', anchor='e')

        self.tree.tag_configure('missing_local', background='#ffcccc', foreground='red')
        self.tree.tag_configure('missing_remote', background='#ffffcc', foreground='darkorange')
        self.tree.tag_configure('mismatch', background='#ff9999', foreground='darkred')
        self.tree.tag_configure('scheduled', background='#ccffcc', foreground='green')
        self.tree.tag_configure('current_growing', background='#e6f3ff', foreground='blue', font=('Segoe UI', 9, 'bold'))
        self.tree.tag_configure('conn_failed', background='#e0e0e0', foreground='#666666')

        # TAB 2: Network Summary
        tab2 = ttk.Frame(self.notebook)
        self.notebook.add(tab2, text=" Network Summary ")

        sum_ctrl = ttk.LabelFrame(tab2, text=" Summary Settings ")
        sum_ctrl.pack(fill='x', padx=15, pady=10)
        ttk.Label(sum_ctrl, text="Last").pack(side=tk.LEFT, padx=10)
        days_spin = ttk.Spinbox(sum_ctrl, from_=1, to=365, textvariable=self.summary_days_var, width=6)
        days_spin.pack(side=tk.LEFT, padx=5)
        ttk.Label(sum_ctrl, text="days").pack(side=tk.LEFT)
        ttk.Label(sum_ctrl, text="  Show:").pack(side=tk.LEFT, padx=15)
        self.summary_combo = ttk.Combobox(sum_ctrl, textvariable=self.summary_filter, state='readonly', width=35)
        self.summary_combo.pack(side=tk.LEFT, padx=5)
        self.summary_combo.bind('<<ComboboxSelected>>', lambda e: self._refresh_summary())
        ttk.Button(sum_ctrl, text="SCAN & REFRESH SUMMARY", command=self._refresh_summary_full, style="Accent.TButton").pack(side=tk.LEFT, padx=20)
        ttk.Checkbutton(sum_ctrl, text="Auto-Refresh after download", variable=self.auto_refresh).pack(side=tk.LEFT, padx=30)

        sum_paned = ttk.PanedWindow(tab2, orient=tk.HORIZONTAL)
        sum_paned.pack(fill='both', expand=True, padx=15, pady=10)

        tree_frame_sum = ttk.Frame(sum_paned)
        sum_paned.add(tree_frame_sum, weight=1)
        h_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient='horizontal')
        self.summary_tree = ttk.Treeview(tree_frame_sum, columns=('Group','Last Download','Last File','Missing Count'),
                                        show='headings', xscrollcommand=h_scroll_sum.set)
        h_scroll_sum.config(command=self.summary_tree.xview)
        h_scroll_sum.pack(side='bottom', fill='x')
        self.summary_tree.pack(fill='both', expand=True)

        widths_sum = [520,180,220,110]
        for c, w in zip(self.summary_tree['columns'], widths_sum):
            self.summary_tree.heading(c, text=c)
            self.summary_tree.column(c, width=w, anchor='center')
        self.summary_tree.column('Group', anchor='w')

        missing_frame = ttk.LabelFrame(sum_paned, text=" ROLLING MISSING FILES LIST (LIVE & 100% ACCURATE) ")
        sum_paned.add(missing_frame, weight=1)
        self.missing_text = scrolledtext.ScrolledText(missing_frame, width=70, height=30, font=('Consolas', 10), bg='#fff8f8', wrap='none')
        h_scroll_text = ttk.Scrollbar(missing_frame, orient='horizontal', command=self.missing_text.xview)
        self.missing_text.configure(xscrollcommand=h_scroll_text.set)
        self.missing_text.pack(fill='both', expand=True, padx=10, pady=(10,0))
        h_scroll_text.pack(fill='x', padx=10, pady=(0,10))

        self.summary_tree.tag_configure('ok', foreground='darkgreen')
        self.summary_tree.tag_configure('missing', foreground='red', font=('Segoe UI', 9, 'bold'))
        self.summary_tree.bind('<<TreeviewSelect>>', self._show_missing_details)

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

    def _show_missing_details(self, event=None):
        sel = self.summary_tree.selection()
        self.missing_text.delete(1.0, tk.END)
        if not sel:
            self.missing_text.insert(tk.END, "Click a station above to view missing files...")
            return
        item = self.summary_tree.item(sel[0])
        values = item['values']
        group = values[0]
        missing_files = self._missing_by_iid.get(sel[0], [])
        if not missing_files:
            self.missing_text.insert(tk.END, f"PERFECT! NO MISSING FILES\n\n{group}\n\nAll files present and correct!")
            return
        self.missing_text.insert(tk.END, f"MISSING FILES FOR:\n{group}\n\n")
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

        days = self.summary_days_var.get()
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
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
                    groups[group_key] = {'last_dt': None, 'last_file': '', 'missing': []}
                
                file_dt = item.get('file_dt')
                if not file_dt and item['local'] == 'yes' and os.path.exists(item['local_path']):
                    try:
                        mtime = os.path.getmtime(item['local_path'])
                        file_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
                        item['file_dt'] = file_dt
                    except OSError:
                        pass
                
                if file_dt and (groups[group_key]['last_dt'] is None or file_dt > groups[group_key]['last_dt']):
                    groups[group_key]['last_dt'] = file_dt
                    groups[group_key]['last_file'] = item['file']

                if item['status'] in ('missing locally', 'missing remotely', 'size mismatch') and not item.get('is_current_utc', False):
                    if file_dt is None or file_dt >= cutoff:
                        groups[group_key]['missing'].append(item['file'])

        self._missing_by_iid.clear()
        for group, data in sorted(groups.items()):
            last_str = data['last_dt'].strftime("%Y-%m-%d %H:%M UTC") if data['last_dt'] else "Never"
            missing_files = data['missing']
            missing_count = len(missing_files)
            tag = 'ok' if missing_count == 0 else 'missing'
            iid = self.summary_tree.insert('', 'end', values=(
                group, last_str, data['last_file'] or "—", missing_count
            ), tags=(tag,))
            self._missing_by_iid[iid] = missing_files

        total_missing = sum(len(g['missing']) for g in groups.values())
        filter_text = f" (filtered: {target_log})" if target_log else ""
        self.status_var.set(f"Summary: {len(groups)} stations | {total_missing} missing files{filter_text}")

    def _refresh_summary_full(self):
        self.status_var.set("Scanning full Greek network...")
        def task():
            log = self.manager.scan_all(self.summary_days_var.get())
            self.full_log = log
            self.root.after(0, self._refresh_summary)
            self.root.after(0, lambda: self.status_var.set("Summary updated – v9.999.9.7 100% ACCURATE"))
        threading.Thread(target=task, daemon=True).start()

    def _refresh_after_download(self):
        if self.auto_refresh.get():
            self._refresh_table()
            self._refresh_summary()

    def _insert_site_items(self, site_items):
        now_utc = datetime.now(timezone.utc)
        for item in site_items:
            if self.show_issues.get() and item['status'] in ['ok', 'scheduled'] and not item.get('is_current_utc'):
                continue
            if self.filter_site.get() != "All Stations" and item['site'] != self.filter_site.get():
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
                   'missing_local' if view_item['status'] == 'missing locally' else
                   'missing_remote' if view_item['status'] == 'missing remotely' else
                   'mismatch' if view_item['status'] == 'size mismatch' else 'scheduled')

            log_name = view_item['site']
            station_name = getattr(view_item['site_obj'], 'station_code', extract_station_name(view_item['file']))
            local_size_str = format_size(view_item['local_size']) if view_item['local'] == 'yes' else "\u2014"
            remote_size_str = format_size(view_item['remote_size']) if view_item['remote'] == 'yes' else "\u2014"

            self.tree.insert('', 'end', values=(
                log_name, station_name, view_item['date'], view_item['file'],
                view_item['local'], local_size_str,
                view_item['remote'], remote_size_str,
                view_item['status'],
                'CURRENT (growing)' if view_item.get('is_current_utc') else 'Future' if view_item['future'] else 'Past'
            ), tags=(tag,))

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
                log = self.manager.scan_all(
                    self.days_var.get(),
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
                flags = (subprocess.CREATE_NO_WINDOW |
                         subprocess.DETACHED_PROCESS |
                         subprocess.CREATE_NEW_PROCESS_GROUP |
                         subprocess.CREATE_BREAKAWAY_FROM_JOB)
                try:
                    subprocess.Popen([python, script, '--headless'],
                                     creationflags=flags, close_fds=True)
                except OSError:
                    # CREATE_BREAKAWAY_FROM_JOB denied — fall back without it
                    subprocess.Popen([python, script, '--headless'],
                                     creationflags=flags & ~subprocess.CREATE_BREAKAWAY_FROM_JOB,
                                     close_fds=True)
                logger.info("Launched scheduler service process")
                newly_launched = True

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
                    startup_deadline = time.time() + 5  # grace period for the restarted process
                else:
                    # Service stopped itself
                    self.root.after(0, self._on_service_stopped)
                    return

            # Update control file with current GUI settings
            write_control(True, self.delay_minutes.get(), self.days_var.get())

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
        self.scheduler_var.set("Service stopped")

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

    def _refresh_sites(self):
        def build_tree():
            networks = {}
            for site in self.manager.sites:
                net = site.network or "Unknown"
                if net not in networks:
                    networks[net] = {}
                station = site.station_code or site.name
                rate_key = f"{site.rate} {'[ExtClk]' if site.external_clock else ''}".strip()
                key = f"{station} | {site.name} | {rate_key}"
                if key not in networks[net]:
                    networks[net][key] = []
                networks[net][key].append(site)
            return networks

        def update_ui(networks):
            for item in self.tree_sites.get_children():
                self.tree_sites.delete(item)

            for net, stations in sorted(networks.items()):
                net_id = self.tree_sites.insert('', 'end', text=f" {net.upper()}", open=True)
                for station_key, sites in sorted(stations.items()):
                    parts = station_key.split(' | ')
                    station_name = parts[0]
                    log_name = parts[1]
                    rate = parts[2]
                    station_id = self.tree_sites.insert(net_id, 'end', text=f"  {station_name}", open=True)
                    self.tree_sites.insert(station_id, 'end', text=f"   {log_name} - {rate}", values=(log_name,))

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

        # Pattern reference
        ref_frame = ttk.LabelFrame(win, text=" Pattern Reference (strftime codes) ")
        ref_frame.grid(row=len(fields)+2, column=0, columnspan=2, padx=20, pady=10, sticky='ew')
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
            try:
                if site:
                    self.manager.edit_site(idx, **data)
                else:
                    self.manager.add_site(**data)
                self._refresh_sites()
                win.destroy()
            except Exception as e:
                messagebox.showerror("Error", str(e))
        ttk.Button(win, text="Save Station", command=save).grid(row=len(fields)+3, column=0, columnspan=2, pady=20)

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

    def _on_close(self):
        if self.scheduler_running and is_service_running():
            messagebox.showinfo(
                "Scheduler Running",
                "The scheduler service will keep running in the background.\n"
                "Reopen the app to monitor or stop it."
            )
        self.root.destroy()

    def run(self):
        self.root.mainloop()
