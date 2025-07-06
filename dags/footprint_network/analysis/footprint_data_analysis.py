"""
Global Footprint Network Data Analysis
=====================================

This script provides data analysis and visualization for the transformed 
Global Footprint Network data, showcasing key ecological indicators, 
geographical patterns, and time trends.
"""
import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import cm
import logging
from datetime import datetime

# Add the parent directory to import the utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_transformer_core import FootprintCoreTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('footprint_data_analysis')

# Create output directories if they don't exist
plots_dir = os.path.join(parent_dir, 'data', 'plots')
os.makedirs(plots_dir, exist_ok=True)

reports_dir = os.path.join(parent_dir, 'data', 'reports')
os.makedirs(reports_dir, exist_ok=True)

class FootprintDataAnalysis:
    """
    Analyze and visualize Global Footprint Network data.
    """
    
    def __init__(self):
        """
        Initialize the analysis class by loading transformed data.
        """
        self.transformer = FootprintCoreTransformer()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.data = {}
        
        # Load all transformed datasets
        logger.info("Loading transformed datasets...")
        self.load_all_datasets()
    
    def load_all_datasets(self):
        """
        Load all transformed datasets into memory.
        """
        try:
            all_data = self.transformer.run_all_core_transformations()
            self.data = all_data
            
            logger.info(f"Loaded {len(self.data)} transformed datasets")
            for name, df in self.data.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    logger.info(f"  {name}: {len(df)} rows, {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Error loading datasets: {str(e)}")
            raise
    
    def save_plot(self, fig, filename, dpi=300):
        """
        Save a matplotlib figure to the plots directory.
        
        Args:
            fig: The matplotlib figure
            filename: The base filename without extension
            dpi: The resolution for the saved figure
        """
        full_path = os.path.join(plots_dir, f"{filename}_{self.timestamp}.png")
        fig.savefig(full_path, dpi=dpi, bbox_inches='tight')
        logger.info(f"Saved plot to {full_path}")
        plt.close(fig)
    
    def generate_summary_report(self):
        """
        Generate a text summary report with key statistics.
        """
        report_lines = [
            "# Global Footprint Network Data Analysis Summary",
            f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n## Dataset Sizes",
        ]
        
        for name, df in self.data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                report_lines.append(f"* {name}: {len(df)} rows, {len(df.columns)} columns")
        
        # Add ecological balance summary
        if 'indicator_ecological_balance' in self.data:
            balance_df = self.data['indicator_ecological_balance']
            if not balance_df.empty:
                report_lines.extend([
                    "\n## Ecological Balance Summary",
                    f"* Global average ecological balance: {balance_df['ecological_balance'].mean():.2f} gha/person",
                    f"* Countries with ecological reserve: {(balance_df['ecological_balance'] > 0).sum()}",
                    f"* Countries with ecological deficit: {(balance_df['ecological_balance'] < 0).sum()}",
                    "\n### Top 5 Countries with Largest Ecological Reserve:",
                ])
                
                top_reserve = balance_df.sort_values('ecological_balance', ascending=False).head(5)
                for _, row in top_reserve.iterrows():
                    report_lines.append(f"* {row['country_name']}: {row['ecological_balance']:.2f} gha/person")
                
                report_lines.append("\n### Top 5 Countries with Largest Ecological Deficit:")
                top_deficit = balance_df.sort_values('ecological_balance').head(5)
                for _, row in top_deficit.iterrows():
                    report_lines.append(f"* {row['country_name']}: {row['ecological_balance']:.2f} gha/person")
        
        # Add footprint composition summary
        if 'indicator_footprint_composition' in self.data:
            comp_df = self.data['indicator_footprint_composition']
            if not comp_df.empty:
                report_lines.extend([
                    "\n## Footprint Composition Summary",
                    "### Average Component Percentages:",
                ])
                
                for col in ['carbon_pct', 'crop_land_pct', 'grazing_land_pct', 
                           'forest_land_pct', 'fishing_ground_pct', 'builtup_land_pct']:
                    if col in comp_df.columns:
                        report_lines.append(f"* {col.replace('_pct', '')}: {comp_df[col].mean():.1f}%")
        
        # Add regional aggregation summary
        if 'region_aggregations' in self.data:
            region_df = self.data['region_aggregations']
            if not region_df.empty:
                # Filter to the latest year and biocapacity per capita
                latest_year = region_df['year'].max()
                biocap_by_region = region_df[
                    (region_df['year'] == latest_year) & 
                    (region_df['record'] == 'BiocapPerCap')
                ]
                
                if not biocap_by_region.empty:
                    report_lines.extend([
                        f"\n## Regional Biocapacity Summary ({latest_year})",
                    ])
                    
                    for _, row in biocap_by_region.sort_values('value_mean', ascending=False).iterrows():
                        report_lines.append(
                            f"* {row['region']}: {row['value_mean']:.2f} gha/person (n={row['value_count']:.0f})"
                        )
        
        # Write the report to a file
        report_path = os.path.join(reports_dir, f"footprint_analysis_summary_{self.timestamp}.md")
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"Saved summary report to {report_path}")
        return report_lines
    
    def plot_ecological_balance_map(self):
        """
        Create a visualization of ecological balance by country.
        """
        if 'indicator_ecological_balance' not in self.data:
            logger.warning("Ecological balance data not available")
            return
        
        balance_df = self.data['indicator_ecological_balance']
        if balance_df.empty:
            logger.warning("Ecological balance dataframe is empty")
            return
        
        try:
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # Sort by ecological balance for better visualization
            plot_df = balance_df.sort_values('ecological_balance')
            
            # Create horizontal bar chart
            bars = sns.barplot(
                x='ecological_balance', 
                y='country_name',
                data=plot_df.head(25),  # Top 25 deficit countries
                palette='coolwarm_r',
                ax=ax
            )
            
            # Add vertical line at zero
            ax.axvline(x=0, color='black', linestyle='-', alpha=0.3)
            
            # Customize plot
            ax.set_title('Top 25 Countries with Largest Ecological Deficit', fontsize=14)
            ax.set_xlabel('Ecological Balance (gha/person)', fontsize=12)
            ax.set_ylabel('Country', fontsize=12)
            
            self.save_plot(fig, "ecological_deficit_countries")
            
            # Also create a plot for countries with largest reserve
            fig, ax = plt.subplots(figsize=(14, 8))
            
            bars = sns.barplot(
                x='ecological_balance', 
                y='country_name',
                data=plot_df.sort_values('ecological_balance', ascending=False).head(25),  # Top 25 reserve countries
                palette='coolwarm',
                ax=ax
            )
            
            # Add vertical line at zero
            ax.axvline(x=0, color='black', linestyle='-', alpha=0.3)
            
            # Customize plot
            ax.set_title('Top 25 Countries with Largest Ecological Reserve', fontsize=14)
            ax.set_xlabel('Ecological Balance (gha/person)', fontsize=12)
            ax.set_ylabel('Country', fontsize=12)
            
            self.save_plot(fig, "ecological_reserve_countries")
            
        except Exception as e:
            logger.error(f"Error creating ecological balance map: {str(e)}")
    
    def plot_footprint_composition(self):
        """
        Create visualizations for footprint composition.
        """
        if 'indicator_footprint_composition' not in self.data or 'dim_countries' not in self.data:
            logger.warning("Footprint composition or countries data not available")
            return
        
        comp_df = self.data['indicator_footprint_composition']
        countries_df = self.data['dim_countries']
        
        if comp_df.empty or countries_df.empty:
            logger.warning("Footprint composition or countries dataframe is empty")
            return
        
        try:
            # Merge with countries data to get region and income group
            merged_df = pd.merge(
                comp_df,
                countries_df[['country_code', 'country_name', 'region', 'income_group']],
                on='country_code',
                how='left'
            )
            
            # 1. Plot carbon dependency by region
            fig, ax = plt.subplots(figsize=(12, 6))
            
            sns.boxplot(
                x='region', 
                y='carbon_dependency', 
                data=merged_df,
                palette='viridis',
                ax=ax
            )
            
            ax.set_title('Carbon Dependency by Region', fontsize=14)
            ax.set_xlabel('Region', fontsize=12)
            ax.set_ylabel('Carbon Dependency (%)', fontsize=12)
            ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
            
            self.save_plot(fig, "carbon_dependency_by_region")
            
            # 2. Plot average footprint composition for all countries
            components = [
                'carbon_pct', 'crop_land_pct', 'grazing_land_pct', 
                'forest_land_pct', 'fishing_ground_pct', 'builtup_land_pct'
            ]
            
            # Calculate average percentages
            avg_comp = {col.replace('_pct', ''): merged_df[col].mean() for col in components if col in merged_df.columns}
            
            fig, ax = plt.subplots(figsize=(10, 10))
            
            # Create pie chart
            wedges, texts, autotexts = ax.pie(
                avg_comp.values(), 
                labels=[k.replace('_', ' ').title() for k in avg_comp.keys()],
                autopct='%1.1f%%',
                startangle=90,
                shadow=False,
                colors=sns.color_palette('viridis', len(avg_comp))
            )
            
            # Equal aspect ratio ensures that pie is drawn as a circle
            ax.axis('equal')
            ax.set_title('Global Average Ecological Footprint Composition', fontsize=14)
            
            # Make text properties prettier
            plt.setp(autotexts, size=12, weight="bold")
            plt.setp(texts, size=12)
            
            self.save_plot(fig, "global_footprint_composition")
            
            # 3. Plot footprint composition by income group as stacked bars
            pivot_df = merged_df.groupby('income_group')[components].mean().reset_index()
            
            # Convert to long format for stacked bar chart
            plot_df = pd.melt(
                pivot_df, 
                id_vars=['income_group'],
                value_vars=components,
                var_name='component',
                value_name='percentage'
            )
            
            # Clean up component names
            plot_df['component'] = plot_df['component'].apply(
                lambda x: x.replace('_pct', '').replace('_', ' ').title()
            )
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Create stacked bar chart
            sns.barplot(
                x='income_group', 
                y='percentage', 
                hue='component',
                data=plot_df,
                palette='viridis',
                ax=ax
            )
            
            ax.set_title('Footprint Composition by Income Group', fontsize=14)
            ax.set_xlabel('Income Group', fontsize=12)
            ax.set_ylabel('Percentage (%)', fontsize=12)
            ax.legend(title='Component', title_fontsize=12, fontsize=10)
            
            self.save_plot(fig, "footprint_composition_by_income")
            
        except Exception as e:
            logger.error(f"Error creating footprint composition plots: {str(e)}")
    
    def plot_biocap_vs_footprint(self):
        """
        Create visualizations comparing biocapacity and ecological footprint.
        """
        if ('fact_ecological_measures' not in self.data or 
            'dim_countries' not in self.data or
            'dim_record_types' not in self.data):
            logger.warning("Required data not available")
            return
        
        measures_df = self.data['fact_ecological_measures']
        countries_df = self.data['dim_countries']
        record_types_df = self.data['dim_record_types']
        
        if measures_df.empty or countries_df.empty or record_types_df.empty:
            logger.warning("Required dataframe is empty")
            return
        
        try:
            # Filter for the latest year
            latest_year = measures_df['year'].max()
            latest_data = measures_df[measures_df['year'] == latest_year]
            
            # Filter for BiocapPerCap and EFConsPerCap records
            biocap_ef_data = latest_data[latest_data['record'].isin(['BiocapPerCap', 'EFConsPerCap'])]
            
            # Pivot to get biocapacity and footprint as separate columns
            pivot_df = biocap_ef_data.pivot_table(
                index=['country_code'],
                columns='record',
                values='value'
            ).reset_index()
            
            # Merge with countries data to get region and income group
            merged_df = pd.merge(
                pivot_df,
                countries_df[['country_code', 'country_name', 'region', 'income_group', 'population']],
                on='country_code',
                how='inner'
            )
            
            # Create scatter plot of biocapacity vs footprint
            fig, ax = plt.subplots(figsize=(12, 10))
            
            # Calculate point sizes based on population (sqrt for better visualization)
            merged_df['point_size'] = np.sqrt(merged_df['population']) / 500
            
            # Create scatter plot with region-based colors
            for region, group in merged_df.groupby('region'):
                ax.scatter(
                    x=group['BiocapPerCap'],
                    y=group['EFConsPerCap'],
                    s=group['point_size'],
                    alpha=0.7,
                    label=region
                )
            
            # Add diagonal line representing balance point
            max_val = max(merged_df['BiocapPerCap'].max(), merged_df['EFConsPerCap'].max()) * 1.1
            ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.5)
            
            # Label countries of interest
            for _, row in merged_df.nlargest(5, 'population').iterrows():
                ax.annotate(
                    row['country_name'],
                    xy=(row['BiocapPerCap'], row['EFConsPerCap']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=9
                )
            
            # Add extreme cases
            for _, row in merged_df.nlargest(3, 'BiocapPerCap').iterrows():
                ax.annotate(
                    row['country_name'],
                    xy=(row['BiocapPerCap'], row['EFConsPerCap']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=9
                )
            
            for _, row in merged_df.nlargest(3, 'EFConsPerCap').iterrows():
                ax.annotate(
                    row['country_name'],
                    xy=(row['BiocapPerCap'], row['EFConsPerCap']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=9
                )
            
            # Customize plot
            ax.set_title(f'Biocapacity vs. Ecological Footprint by Country ({latest_year})', fontsize=14)
            ax.set_xlabel('Biocapacity per Capita (gha/person)', fontsize=12)
            ax.set_ylabel('Ecological Footprint per Capita (gha/person)', fontsize=12)
            ax.legend(title='Region', title_fontsize=12)
            
            # Add explanatory text for quadrants
            ax.text(max_val*0.75, max_val*0.2, 'Ecological Reserve', fontsize=9, ha='center')
            ax.text(max_val*0.25, max_val*0.8, 'Ecological Deficit', fontsize=9, ha='center')
            
            self.save_plot(fig, "biocapacity_vs_footprint")
            
        except Exception as e:
            logger.error(f"Error creating biocap vs footprint plot: {str(e)}")
    
    def plot_region_aggregations(self):
        """
        Create visualizations for regional aggregations.
        """
        if 'region_aggregations' not in self.data:
            logger.warning("Region aggregations data not available")
            return
        
        region_df = self.data['region_aggregations']
        
        if region_df.empty:
            logger.warning("Region aggregations dataframe is empty")
            return
        
        try:
            # Filter for key records: BiocapPerCap, EFConsPerCap
            key_records = ['BiocapPerCap', 'EFConsPerCap']
            plot_df = region_df[region_df['record'].isin(key_records)]
            
            # Get all available years and sort
            years = sorted(plot_df['year'].unique())
            
            # Filter for a few specific years to see trends (first, middle, last)
            if len(years) >= 3:
                plot_years = [years[0], years[len(years)//2], years[-1]]
            else:
                plot_years = years
            
            plot_df = plot_df[plot_df['year'].isin(plot_years)]
            
            # Create grouped bar chart
            fig, ax = plt.subplots(figsize=(15, 10))
            
            # Set positions and width for grouped bars
            bar_width = 0.35
            index = np.arange(len(plot_df['region'].unique()))
            
            # Plot each year and record type
            for i, year in enumerate(plot_years):
                year_data = plot_df[plot_df['year'] == year]
                
                # BiocapPerCap
                biocap_data = year_data[year_data['record'] == 'BiocapPerCap'].sort_values('region')
                if not biocap_data.empty:
                    ax.bar(
                        index + (i - 0.5) * bar_width, 
                        biocap_data['value_mean'], 
                        bar_width,
                        alpha=0.7,
                        label=f'Biocapacity {year}'
                    )
                
                # EFConsPerCap
                ef_data = year_data[year_data['record'] == 'EFConsPerCap'].sort_values('region')
                if not ef_data.empty:
                    ax.bar(
                        index + i * bar_width, 
                        ef_data['value_mean'], 
                        bar_width,
                        alpha=0.7,
                        label=f'Footprint {year}'
                    )
            
            # Customize plot
            ax.set_title('Biocapacity vs. Ecological Footprint by Region Over Time', fontsize=14)
            ax.set_xlabel('Region', fontsize=12)
            ax.set_ylabel('Global Hectares per Capita (gha/person)', fontsize=12)
            ax.set_xticks(index)
            ax.set_xticklabels(biocap_data['region'].tolist(), rotation=45, ha='right')
            ax.legend(fontsize=10)
            
            self.save_plot(fig, "region_biocap_vs_footprint_trend")
            
        except Exception as e:
            logger.error(f"Error creating region aggregations plot: {str(e)}")
    
    def run_all_analyses(self):
        """
        Run all analysis and visualization functions.
        """
        logger.info("Starting comprehensive data analysis")
        
        # Generate summary report
        self.generate_summary_report()
        
        # Create visualizations
        self.plot_ecological_balance_map()
        self.plot_footprint_composition()
        self.plot_biocap_vs_footprint()
        self.plot_region_aggregations()
        
        logger.info("Data analysis complete")

if __name__ == "__main__":
    try:
        logger.info("Starting Global Footprint Network data analysis")
        analyzer = FootprintDataAnalysis()
        analyzer.run_all_analyses()
        logger.info("Analysis completed successfully")
    except Exception as e:
        logger.error(f"Error in footprint data analysis: {str(e)}")
        raise
