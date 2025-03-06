import psycopg2
from tabulate import tabulate

def get_connection():
    return psycopg2.connect(
        database='textile_quality_control',
        user='admin',
        password='admin_password',
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
        qc.PassStatus,
        COUNT(d.DefectID) AS DefectCount,
        ROUND(ca.AvgDefectsPerCategory, 2) AS CategoryAverage,
        CASE
            WHEN COUNT(d.DefectID) > ca.AvgDefectsPerCategory THEN 'Above Average'
            WHEN COUNT(d.DefectID) < ca.AvgDefectsPerCategory THEN 'Below Average'
            ELSE 'Average'
        END AS DefectStatus,
        RANK() OVER (
            PARTITION BY p.Category 
            ORDER BY SUM(d.Severity * d.Severity) DESC  -- Weighted severity calculation
        ) AS SeverityRank
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
                
                print("\nQuality Control Analysis Report:")
                print(tabulate(results, headers=columns, tablefmt="grid", numalign="center"))
                
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    quality_analysis_report()