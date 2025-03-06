import psycopg2
import csv
from datetime import datetime

def get_connection():  
    return psycopg2.connect(  
        database='textile_quality_control',  
        user='admin',  
        password='admin_password',  
        host='localhost',  
        port='5432'  
    )  

def quality_analysis_report():  
    query = """  
    WITH DailyDefects AS (  
        SELECT  
            p.Category,  
            qc.CheckDate,  
            COUNT(d.DefectID) AS DefectCount  
        FROM Products p  
        JOIN QualityChecks qc ON p.ProductID = qc.ProductID  
        LEFT JOIN Defects d ON qc.CheckID = d.CheckID  
        GROUP BY p.Category, qc.CheckDate  
    ),  
    CategoryAverages AS (  
        SELECT  
            Category,  
            AVG(DefectCount) AS AvgDefectsPerCategory  
        FROM DailyDefects  
        GROUP BY Category  
    )  
    SELECT  
        p.ProductName,  
        p.Category,  
        qc.CheckDate,  
        CASE WHEN qc.PassStatus THEN 'Pass' ELSE 'Fail' END AS Status,
        COUNT(d.DefectID) AS DefectCount,  
        ROUND(ca.AvgDefectsPerCategory, 2) AS CategoryAverage,  
        CASE  
            WHEN COUNT(d.DefectID) > ca.AvgDefectsPerCategory THEN 'Above Average'  
            WHEN COUNT(d.DefectID) < ca.AvgDefectsPerCategory THEN 'Below Average'  
            ELSE 'Average'  
        END AS DefectStatus  
    FROM Products p  
    JOIN QualityChecks qc ON p.ProductID = qc.ProductID  
    LEFT JOIN Defects d ON qc.CheckID = d.CheckID  
    JOIN CategoryAverages ca ON p.Category = ca.Category  
    GROUP BY p.ProductName, p.Category, qc.CheckDate, qc.PassStatus, ca.AvgDefectsPerCategory;  
    """  
    
    try:  
        with get_connection() as conn:  
            with conn.cursor() as cur:  
                cur.execute(query)  
                results = cur.fetchall()  
                columns = [desc[0] for desc in cur.description]
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"quality_report_{timestamp}.csv"
                
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(results)
                    
                print(f"Report successfully saved to {filename}")

    except Exception as e:  
        print(f"Error: {e}")  

if __name__ == "__main__":  
    quality_analysis_report()