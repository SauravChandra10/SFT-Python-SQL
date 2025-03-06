import os
import pandas as pd
from sqlalchemy import create_engine
from tabulate import tabulate
from bokeh.io import show, output_file
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool

db_string = "postgresql+psycopg2://admin:admin_password@localhost:5432/water_quality_db"
engine = create_engine(db_string)

sql_query = """
WITH industry_summary AS (  
    SELECT  
        location_id,  
        industry_type,  
        AVG(emission_level) AS avg_emission  
    FROM industrial_activity  
    GROUP BY location_id, industry_type  
),  
water_summary AS (  
    SELECT  
        location_id,  
        AVG(measurement_value) AS avg_measurement  
    FROM water_quality  
    GROUP BY location_id  
)  
SELECT  
    l.site_name,  
    l.region,  
    i.industry_type,  
    i.avg_emission,  
    w.avg_measurement,  
    (i.avg_emission / NULLIF(w.avg_measurement, 0)) AS emission_to_quality_ratio  
FROM industry_summary i  
JOIN water_summary w ON i.location_id = w.location_id  
JOIN locations l ON l.location_id = i.location_id;  
"""

df = pd.read_sql_query(sql_query, engine)

print(tabulate(df, headers='keys', tablefmt='psql'))

source = ColumnDataSource(df)

p = figure(
    x_range=df['site_name'].tolist(), 
    title="Emission to Water Quality Ratio per Site",
    x_axis_label='Site Name', 
    y_axis_label='Emission to Quality Ratio',
)

p.vbar(
    x='site_name', 
    top='emission_to_quality_ratio', 
    width=0.8, 
    source=source, 
    legend_field='industry_type', 
    line_color='white', 
    fill_color='navy'
)

p.legend.title = "Industry Type"

output_dir = "output_graph"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file(os.path.join(output_dir, "emission_quality_ratio.html"))
show(p)