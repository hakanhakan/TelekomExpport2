#!/usr/bin/env python3
"""
Monitor IBT Property Extraction Progress
This script provides a visual interface to monitor the progress of property extraction.
"""

import asyncio
import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live

# Global variables
console = Console()

class ExtractionMonitor:
    """Monitors the progress of property extraction"""
    
    def __init__(self, db_path: str, refresh_interval: int = 5):
        """Initialize the monitor"""
        self.db_path = db_path
        self.refresh_interval = refresh_interval
        self.conn = None
        self.running = False
        
    def connect_to_db(self) -> bool:
        """Connect to the SQLite database"""
        try:
            if not Path(self.db_path).exists():
                console.print(f"[bold red]Database file not found: {self.db_path}[/bold red]")
                return False
                
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            return True
        except Exception as e:
            console.print(f"[bold red]Error connecting to database: {str(e)}[/bold red]")
            return False
            
    def get_progress_stats(self):
        """Get progress statistics from the database"""
        try:
            cursor = self.conn.cursor()
            
            # Get property stats
            cursor.execute(
                '''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM properties
                '''
            )
            property_stats = dict(cursor.fetchone())
            
            # Get session stats
            cursor.execute(
                '''
                SELECT 
                    session_id,
                    status,
                    properties_processed,
                    properties_failed,
                    datetime(last_active) as last_active
                FROM sessions
                ORDER BY session_id
                '''
            )
            sessions = [dict(row) for row in cursor.fetchall()]
            
            # Get recently completed properties
            cursor.execute(
                '''
                SELECT 
                    property_id,
                    address,
                    city,
                    owner_name,
                    owner_email,
                    datetime(last_updated) as completed_at
                FROM properties
                WHERE status = 'completed'
                ORDER BY last_updated DESC
                LIMIT 5
                '''
            )
            recent_completed = [dict(row) for row in cursor.fetchall()]
            
            # Get recently failed properties
            cursor.execute(
                '''
                SELECT 
                    property_id,
                    error_message,
                    attempts,
                    datetime(last_updated) as failed_at
                FROM properties
                WHERE status = 'failed'
                ORDER BY last_updated DESC
                LIMIT 5
                '''
            )
            recent_failed = [dict(row) for row in cursor.fetchall()]
            
            # Get processing rate
            cursor.execute(
                '''
                SELECT 
                    COUNT(*) as count,
                    MIN(datetime(last_updated)) as start_time,
                    MAX(datetime(last_updated)) as end_time
                FROM properties
                WHERE status = 'completed'
                '''
            )
            rate_data = dict(cursor.fetchone())
            
            processing_rate = None
            if rate_data['count'] > 0 and rate_data['start_time'] and rate_data['end_time']:
                try:
                    start_time = datetime.strptime(rate_data['start_time'], '%Y-%m-%d %H:%M:%S')
                    end_time = datetime.strptime(rate_data['end_time'], '%Y-%m-%d %H:%M:%S')
                    duration = (end_time - start_time).total_seconds()
                    if duration > 0:
                        processing_rate = rate_data['count'] / duration * 60  # properties per minute
                except Exception as e:
                    console.print(f"[bold red]Error calculating processing rate: {str(e)}[/bold red]")
            
            return {
                'property_stats': property_stats,
                'sessions': sessions,
                'recent_completed': recent_completed,
                'recent_failed': recent_failed,
                'processing_rate': processing_rate
            }
            
        except Exception as e:
            console.print(f"[bold red]Error getting progress stats: {str(e)}[/bold red]")
            return None
            
    def create_progress_display(self, stats):
        """Create a rich display of progress information"""
        layout = Layout()
        
        # Create overall progress section
        property_stats = stats['property_stats']
        total = property_stats['total']
        completed = property_stats['completed']
        in_progress = property_stats['in_progress']
        pending = property_stats['pending']
        failed = property_stats['failed']
        
        completion_percentage = 0
        if total > 0:
            completion_percentage = (completed / total) * 100
            
        overall_progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[bold]{task.completed}/{task.total}"),
        )
        
        overall_progress.add_task(
            "Overall Progress", 
            total=total, 
            completed=completed
        )
        
        # Create session progress table
        session_table = Table(title="Worker Sessions")
        session_table.add_column("Session ID")
        session_table.add_column("Status")
        session_table.add_column("Processed")
        session_table.add_column("Failed")
        session_table.add_column("Last Active")
        session_table.add_column("Progress")
        
        for session in stats['sessions']:
            # Create a mini progress bar for each session
            session_progress = Progress(
                BarColumn(),
                TaskProgressColumn(),
                expand=False
            )
            
            # Estimate session progress based on its share of completed properties
            session_completed = session['properties_processed']
            session_total = total // len(stats['sessions'])  # Estimate total per session
            
            session_progress.add_task("", total=session_total, completed=session_completed)
            
            session_table.add_row(
                str(session['session_id']),
                session['status'],
                str(session['properties_processed']),
                str(session['properties_failed']),
                session['last_active'],
                session_progress
            )
            
        # Create statistics panel
        stats_text = f"""
[bold]Total Properties:[/bold] {total}
[bold]Completed:[/bold] {completed} ({completion_percentage:.1f}%)
[bold]In Progress:[/bold] {in_progress}
[bold]Pending:[/bold] {pending}
[bold]Failed:[/bold] {failed}
        """
        
        if stats['processing_rate']:
            stats_text += f"\n[bold]Processing Rate:[/bold] {stats['processing_rate']:.2f} properties/minute"
            
            # Estimate time remaining
            if stats['processing_rate'] > 0:
                remaining = pending + in_progress
                minutes_remaining = remaining / stats['processing_rate']
                hours = int(minutes_remaining // 60)
                minutes = int(minutes_remaining % 60)
                stats_text += f"\n[bold]Estimated Time Remaining:[/bold] {hours}h {minutes}m"
        
        stats_panel = Panel(stats_text, title="Extraction Statistics")
        
        # Create recent completed properties table
        recent_completed_table = Table(title="Recently Completed Properties")
        recent_completed_table.add_column("Property ID")
        recent_completed_table.add_column("Address")
        recent_completed_table.add_column("City")
        recent_completed_table.add_column("Owner Name")
        recent_completed_table.add_column("Owner Email")
        recent_completed_table.add_column("Completed At")
        
        for prop in stats['recent_completed']:
            recent_completed_table.add_row(
                prop['property_id'],
                prop['address'] or "",
                prop['city'] or "",
                prop['owner_name'] or "",
                prop['owner_email'] or "",
                prop['completed_at']
            )
            
        # Create recent failed properties table
        recent_failed_table = Table(title="Recently Failed Properties")
        recent_failed_table.add_column("Property ID")
        recent_failed_table.add_column("Error Message")
        recent_failed_table.add_column("Attempts")
        recent_failed_table.add_column("Failed At")
        
        for prop in stats['recent_failed']:
            recent_failed_table.add_row(
                prop['property_id'],
                prop['error_message'] or "",
                str(prop['attempts']),
                prop['failed_at']
            )
            
        # Arrange layout
        top_section = Layout(Panel(overall_progress, title="Overall Progress"), size=3)
        middle_section = Layout()
        middle_section.split(
            Layout(stats_panel, size=10),
            Layout(session_table)
        )
        bottom_section = Layout()
        bottom_section.split(
            Layout(recent_completed_table),
            Layout(recent_failed_table)
        )
        
        layout.split(
            top_section,
            middle_section,
            bottom_section
        )
        
        return layout
        
    async def run(self):
        """Run the monitor"""
        try:
            if not self.connect_to_db():
                return False
                
            self.running = True
            
            with Live(console=console, refresh_per_second=1) as live:
                while self.running:
                    stats = self.get_progress_stats()
                    if stats:
                        display = self.create_progress_display(stats)
                        live.update(display)
                    
                    # Check if extraction is complete
                    if stats and stats['property_stats']['pending'] == 0 and stats['property_stats']['in_progress'] == 0:
                        # If there are no more pending or in-progress properties, wait a bit longer and then exit
                        await asyncio.sleep(self.refresh_interval)
                        stats = self.get_progress_stats()
                        if stats and stats['property_stats']['pending'] == 0 and stats['property_stats']['in_progress'] == 0:
                            console.print("[bold green]Extraction complete![/bold green]")
                            self.running = False
                            break
                    
                    await asyncio.sleep(self.refresh_interval)
                    
            return True
            
        except KeyboardInterrupt:
            console.print("[yellow]Monitor stopped by user[/yellow]")
            return True
        except Exception as e:
            console.print(f"[bold red]Error in monitor: {str(e)}[/bold red]")
            return False
        finally:
            if self.conn:
                self.conn.close()

async def main():
    """Main entry point for the script"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Monitor IBT Property Extraction Progress")
    parser.add_argument("--db-path", default="property_extraction.db", help="Path to the SQLite database file")
    parser.add_argument("--refresh", type=int, default=5, help="Refresh interval in seconds (default: 5)")
    args = parser.parse_args()
    
    try:
        # Create monitor
        monitor = ExtractionMonitor(
            db_path=args.db_path,
            refresh_interval=args.refresh
        )
        
        # Run monitor
        console.print("[bold cyan]Starting extraction monitor...[/bold cyan]")
        console.print(f"Database: {args.db_path}")
        console.print(f"Refresh interval: {args.refresh} seconds")
        console.print("[yellow]Press Ctrl+C to stop[/yellow]")
        
        await monitor.run()
            
    except Exception as e:
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        return 1
        
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
