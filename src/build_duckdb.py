import duckdb
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

eca_file = os.path.join(project_root, "Data", "eca_reg14_sox_pm.parquet").replace("\\", "/")
mpa_file = os.path.join(project_root, "Data", "marine_polygons.parquet").replace("\\", "/")

db_file = os.path.join(project_root, "eca_mpa.duckdb")

con = duckdb.connect(db_file)
con.execute("INSTALL spatial; LOAD spatial;")

con.execute(f"""
CREATE OR REPLACE TABLE eca_mpa AS
SELECT *, ST_GeomFromWKB(geom_wkb) AS geom
FROM read_parquet('{eca_file}')
UNION ALL
SELECT *, ST_GeomFromWKB(geom_wkb) AS geom
FROM read_parquet('{mpa_file}')
""")

con.execute("CREATE INDEX eca_mpa_spatial_idx ON eca_mpa USING RTREE (geom);")

print(f"Database built and stored at: {db_file}")