#!/usr/bin/env python3
"""
Analyze IBT Property Extraction Results
This script analyzes the results of property extraction and provides insights.
"""

import argparse
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import json

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Global variables
console = Console()

class ResultAnalyzer:
    """Analyzes the results of property extraction"""
    
    def __init__(self, db_path: str, output_dir: str = "analysis"):
        """Initialize the analyzer"""
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.conn = None
        self.df = None
        
    def connect_to_db(self) -> bool:
        """Connect to the SQLite database"""
        try:
            if not Path(self.db_path).exists():
                console.print(f"[bold red]Database file not found: {self.db_path}[/bold red]")
                return False
                
            self.conn = sqlite3.connect(self.db_path)
            return True
        except Exception as e:
            console.print(f"[bold red]Error connecting to database: {str(e)}[/bold red]")
            return False
            
    def load_data(self) -> bool:
        """Load data from the database into a pandas DataFrame"""
        try:
            query = """
            SELECT 
                property_id,
                status,
                address,
                postal_code,
                city,
                property_status,
                owner_name,
                owner_email,
                owner_mobile,
                owner_phone,
                is_decision_maker,
                owner_details_loaded,
                additional_fields,
                attempts,
                error_message
            FROM properties
            """
            
            self.df = pd.read_sql_query(query, self.conn)
            
            # Parse additional_fields JSON
            def parse_additional_fields(json_str):
                if pd.isna(json_str) or not json_str:
                    return {}
                try:
                    return json.loads(json_str)
                except:
                    return {}
                    
            self.df['additional_fields_parsed'] = self.df['additional_fields'].apply(parse_additional_fields)
            
            # Convert boolean integers to boolean
            self.df['is_decision_maker'] = self.df['is_decision_maker'].astype(bool)
            self.df['owner_details_loaded'] = self.df['owner_details_loaded'].astype(bool)
            
            console.print(f"[green]Loaded {len(self.df)} properties from database[/green]")
            return True
        except Exception as e:
            console.print(f"[bold red]Error loading data: {str(e)}[/bold red]")
            return False
            
    def analyze_extraction_status(self):
        """Analyze the status of extraction"""
        try:
            # Count properties by status
            status_counts = self.df['status'].value_counts()
            
            # Create a table
            table = Table(title="Extraction Status")
            table.add_column("Status")
            table.add_column("Count")
            table.add_column("Percentage")
            
            total = len(self.df)
            for status, count in status_counts.items():
                percentage = (count / total) * 100
                table.add_row(
                    status,
                    str(count),
                    f"{percentage:.1f}%"
                )
                
            console.print(table)
            
            # Create a pie chart
            plt.figure(figsize=(10, 6))
            plt.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%')
            plt.title('Extraction Status')
            plt.savefig(self.output_dir / 'extraction_status.png')
            plt.close()
            
            console.print(f"[green]Saved extraction status chart to {self.output_dir / 'extraction_status.png'}[/green]")
            
        except Exception as e:
            console.print(f"[bold red]Error analyzing extraction status: {str(e)}[/bold red]")
            
    def analyze_city_distribution(self):
        """Analyze the distribution of properties by city"""
        try:
            # Filter out properties with no city
            city_df = self.df[self.df['city'].notna()]
            
            # Count properties by city
            city_counts = city_df['city'].value_counts().head(10)  # Top 10 cities
            
            # Create a table
            table = Table(title="Top 10 Cities")
            table.add_column("City")
            table.add_column("Count")
            table.add_column("Percentage")
            
            total = len(city_df)
            for city, count in city_counts.items():
                percentage = (count / total) * 100
                table.add_row(
                    city,
                    str(count),
                    f"{percentage:.1f}%"
                )
                
            console.print(table)
            
            # Create a bar chart
            plt.figure(figsize=(12, 6))
            city_counts.plot(kind='bar')
            plt.title('Top 10 Cities')
            plt.xlabel('City')
            plt.ylabel('Count')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(self.output_dir / 'city_distribution.png')
            plt.close()
            
            console.print(f"[green]Saved city distribution chart to {self.output_dir / 'city_distribution.png'}[/green]")
            
        except Exception as e:
            console.print(f"[bold red]Error analyzing city distribution: {str(e)}[/bold red]")
            
    def analyze_property_status(self):
        """Analyze the distribution of properties by property status"""
        try:
            # Filter out properties with no property status
            status_df = self.df[self.df['property_status'].notna()]
            
            # Count properties by property status
            status_counts = status_df['property_status'].value_counts()
            
            # Create a table
            table = Table(title="Property Status Distribution")
            table.add_column("Property Status")
            table.add_column("Count")
            table.add_column("Percentage")
            
            total = len(status_df)
            for status, count in status_counts.items():
                percentage = (count / total) * 100
                table.add_row(
                    status,
                    str(count),
                    f"{percentage:.1f}%"
                )
                
            console.print(table)
            
            # Create a pie chart
            plt.figure(figsize=(10, 6))
            plt.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%')
            plt.title('Property Status Distribution')
            plt.savefig(self.output_dir / 'property_status_distribution.png')
            plt.close()
            
            console.print(f"[green]Saved property status distribution chart to {self.output_dir / 'property_status_distribution.png'}[/green]")
            
        except Exception as e:
            console.print(f"[bold red]Error analyzing property status distribution: {str(e)}[/bold red]")
            
    def analyze_owner_information(self):
        """Analyze owner information"""
        try:
            # Filter completed properties
            completed_df = self.df[self.df['status'] == 'completed']
            
            # Count properties with owner information
            owner_stats = {
                'With Owner Name': completed_df['owner_name'].notna().sum(),
                'With Owner Email': completed_df['owner_email'].notna().sum(),
                'With Owner Mobile': completed_df['owner_mobile'].notna().sum(),
                'With Owner Phone': completed_df['owner_phone'].notna().sum(),
                'Is Decision Maker': completed_df['is_decision_maker'].sum(),
                'Owner Details Loaded': completed_df['owner_details_loaded'].sum()
            }
            
            # Create a table
            table = Table(title="Owner Information Statistics")
            table.add_column("Metric")
            table.add_column("Count")
            table.add_column("Percentage")
            
            total = len(completed_df)
            for metric, count in owner_stats.items():
                percentage = (count / total) * 100 if total > 0 else 0
                table.add_row(
                    metric,
                    str(count),
                    f"{percentage:.1f}%"
                )
                
            console.print(table)
            
            # Create a bar chart
            plt.figure(figsize=(12, 6))
            metrics = list(owner_stats.keys())
            counts = list(owner_stats.values())
            plt.bar(metrics, counts)
            plt.title('Owner Information Statistics')
            plt.xlabel('Metric')
            plt.ylabel('Count')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(self.output_dir / 'owner_information.png')
            plt.close()
            
            console.print(f"[green]Saved owner information chart to {self.output_dir / 'owner_information.png'}[/green]")
            
        except Exception as e:
            console.print(f"[bold red]Error analyzing owner information: {str(e)}[/bold red]")
            
    def analyze_failure_reasons(self):
        """Analyze reasons for extraction failures"""
        try:
            # Filter failed properties
            failed_df = self.df[self.df['status'] == 'failed']
            
            if len(failed_df) == 0:
                console.print("[green]No failed properties found[/green]")
                return
                
            # Count properties by error message
            error_counts = failed_df['error_message'].value_counts().head(10)  # Top 10 error messages
            
            # Create a table
            table = Table(title="Top 10 Failure Reasons")
            table.add_column("Error Message")
            table.add_column("Count")
            table.add_column("Percentage")
            
            total = len(failed_df)
            for error, count in error_counts.items():
                percentage = (count / total) * 100
                table.add_row(
                    error[:50] + "..." if len(error) > 50 else error,  # Truncate long error messages
                    str(count),
                    f"{percentage:.1f}%"
                )
                
            console.print(table)
            
            # Create a bar chart
            plt.figure(figsize=(12, 6))
            error_counts.plot(kind='bar')
            plt.title('Top 10 Failure Reasons')
            plt.xlabel('Error Message')
            plt.ylabel('Count')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(self.output_dir / 'failure_reasons.png')
            plt.close()
            
            console.print(f"[green]Saved failure reasons chart to {self.output_dir / 'failure_reasons.png'}[/green]")
            
        except Exception as e:
            console.print(f"[bold red]Error analyzing failure reasons: {str(e)}[/bold red]")
            
    def export_summary(self):
        """Export a summary of the analysis"""
        try:
            # Create a summary DataFrame
            summary = {
                'Total Properties': len(self.df),
                'Completed': len(self.df[self.df['status'] == 'completed']),
                'Failed': len(self.df[self.df['status'] == 'failed']),
                'Pending': len(self.df[self.df['status'] == 'pending']),
                'In Progress': len(self.df[self.df['status'] == 'in_progress']),
                'Success Rate': len(self.df[self.df['status'] == 'completed']) / len(self.df) * 100 if len(self.df) > 0 else 0,
                'With Owner Name': len(self.df[self.df['owner_name'].notna()]),
                'With Owner Email': len(self.df[self.df['owner_email'].notna()]),
                'With Owner Mobile': len(self.df[self.df['owner_mobile'].notna()]),
                'With Owner Phone': len(self.df[self.df['owner_phone'].notna()]),
                'Is Decision Maker': self.df['is_decision_maker'].sum(),
                'Owner Details Loaded': self.df['owner_details_loaded'].sum()
            }
            
            # Export to CSV
            summary_df = pd.DataFrame([summary])
            summary_df.to_csv(self.output_dir / 'summary.csv', index=False)
            
            # Export to Excel
            summary_df.to_excel(self.output_dir / 'summary.xlsx', index=False)
            
            console.print(f"[green]Exported summary to {self.output_dir / 'summary.csv'} and {self.output_dir / 'summary.xlsx'}[/green]")
            
            # Display summary
            console.print(Panel(
                f"""
[bold]Total Properties:[/bold] {summary['Total Properties']}
[bold]Completed:[/bold] {summary['Completed']} ({summary['Success Rate']:.1f}%)
[bold]Failed:[/bold] {summary['Failed']}
[bold]Pending:[/bold] {summary['Pending']}
[bold]In Progress:[/bold] {summary['In Progress']}
[bold]With Owner Name:[/bold] {summary['With Owner Name']}
[bold]With Owner Email:[/bold] {summary['With Owner Email']}
[bold]With Owner Mobile:[/bold] {summary['With Owner Mobile']}
[bold]With Owner Phone:[/bold] {summary['With Owner Phone']}
[bold]Is Decision Maker:[/bold] {summary['Is Decision Maker']}
[bold]Owner Details Loaded:[/bold] {summary['Owner Details Loaded']}
                """,
                title="Extraction Summary"
            ))
            
        except Exception as e:
            console.print(f"[bold red]Error exporting summary: {str(e)}[/bold red]")
            
    def run(self):
        """Run the analysis"""
        try:
            console.print("[bold cyan]Starting analysis...[/bold cyan]")
            
            # Connect to the database
            if not self.connect_to_db():
                return False
                
            # Load data
            if not self.load_data():
                return False
                
            # Run analyses
            console.print("\n[bold cyan]Analyzing extraction status...[/bold cyan]")
            self.analyze_extraction_status()
            
            console.print("\n[bold cyan]Analyzing city distribution...[/bold cyan]")
            self.analyze_city_distribution()
            
            console.print("\n[bold cyan]Analyzing property status distribution...[/bold cyan]")
            self.analyze_property_status()
            
            console.print("\n[bold cyan]Analyzing owner information...[/bold cyan]")
            self.analyze_owner_information()
            
            console.print("\n[bold cyan]Analyzing failure reasons...[/bold cyan]")
            self.analyze_failure_reasons()
            
            console.print("\n[bold cyan]Exporting summary...[/bold cyan]")
            self.export_summary()
            
            console.print("\n[bold green]Analysis complete![/bold green]")
            console.print(f"Results saved to {self.output_dir}")
            
            return True
            
        except Exception as e:
            console.print(f"[bold red]Error in analysis: {str(e)}[/bold red]")
            return False
        finally:
            if self.conn:
                self.conn.close()

def main():
    """Main entry point for the script"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Analyze IBT Property Extraction Results")
    parser.add_argument("--db-path", default="property_extraction.db", help="Path to the SQLite database file")
    parser.add_argument("--output-dir", default="analysis", help="Directory to save analysis results")
    args = parser.parse_args()
    
    try:
        # Create analyzer
        analyzer = ResultAnalyzer(
            db_path=args.db_path,
            output_dir=args.output_dir
        )
        
        # Run analysis
        success = analyzer.run()
        
        if success:
            return 0
        else:
            return 1
            
    except Exception as e:
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        return 1

if __name__ == "__main__":
    exit(main())
