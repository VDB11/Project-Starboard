import duckdb
import os
import zipfile
import tempfile
import shutil


def find_shp(directory):
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.endswith(".shp"):
                return os.path.join(root, f).replace("\\", "/")
    return None


def convert_to_parquet():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    eca_zip = os.path.abspath("../Data/eca_reg14_sox_pm.zip")
    mpa_zip = os.path.abspath("../Data/marine_polygons.zip")

    eca_parquet = os.path.abspath("../Data/eca_reg14_sox_pm.parquet").replace("\\", "/")
    mpa_parquet = os.path.abspath("../Data/marine_polygons.parquet").replace("\\", "/")

    tmp_dir = tempfile.mkdtemp()

    try:
        print("Extracting ECA shapefile...")
        eca_extract_dir = os.path.join(tmp_dir, "eca")
        os.makedirs(eca_extract_dir, exist_ok=True)
        with zipfile.ZipFile(eca_zip, 'r') as z:
            z.extractall(eca_extract_dir)

        eca_shp = find_shp(eca_extract_dir)
        if not eca_shp:
            raise FileNotFoundError("No .shp file found inside eca_reg14_sox_pm.zip")

        print(f"  Found: {eca_shp}")
        print("Converting ECA to Parquet...")
        con.execute(f"""
            COPY (
                SELECT
                    ST_AsWKB(geom)                      AS geom_wkb,
                    'ECA'                               AS type,
                    COALESCE(area, 'ECA Area')          AS name,
                    COALESCE(regulation, '')            AS regulation,
                    NULL::VARCHAR                       AS designation,
                    NULL::VARCHAR                       AS iucn_cat,
                    NULL::VARCHAR                       AS status,
                    NULL::VARCHAR                       AS status_yr,
                    NULL::VARCHAR                       AS gov_type,
                    NULL::VARCHAR                       AS iso3,
                    NULL::VARCHAR                       AS marine_area_km2,
                    NULL::VARCHAR                       AS no_take,
                    ST_XMin(geom)                       AS bbox_xmin,
                    ST_YMin(geom)                       AS bbox_ymin,
                    ST_XMax(geom)                       AS bbox_xmax,
                    ST_YMax(geom)                       AS bbox_ymax
                FROM ST_Read('{eca_shp}')
            ) TO '{eca_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"  ECA saved → {eca_parquet}")

        print("Extracting MPA shapefile...")
        mpa_extract_dir = os.path.join(tmp_dir, "mpa")
        os.makedirs(mpa_extract_dir, exist_ok=True)
        with zipfile.ZipFile(mpa_zip, 'r') as z:
            z.extractall(mpa_extract_dir)

        mpa_shp = find_shp(mpa_extract_dir)
        if not mpa_shp:
            raise FileNotFoundError("No .shp file found inside marine_polygons.zip")

        print(f"  Found: {mpa_shp}")
        print("Converting MPA to Parquet...")
        con.execute(f"""
            COPY (
                SELECT
                    ST_AsWKB(geom)                                              AS geom_wkb,
                    'MPA'                                                       AS type,
                    COALESCE(NAME_ENG, NAME, 'MPA Area')                        AS name,
                    NULL::VARCHAR                                               AS regulation,
                    COALESCE(DESIG_ENG, '')                                     AS designation,
                    COALESCE(IUCN_CAT, '')                                      AS iucn_cat,
                    COALESCE(STATUS, '')                                        AS status,
                    CAST(STATUS_YR AS VARCHAR)                                  AS status_yr,
                    COALESCE(GOV_TYPE, '')                                      AS gov_type,
                    COALESCE(ISO3, '')                                          AS iso3,
                    CAST(ROUND(COALESCE(REP_M_AREA, GIS_M_AREA, 0), 2)
                         AS VARCHAR)                                            AS marine_area_km2,
                    COALESCE(NO_TAKE, '')                                       AS no_take,
                    ST_XMin(geom)                                               AS bbox_xmin,
                    ST_YMin(geom)                                               AS bbox_ymin,
                    ST_XMax(geom)                                               AS bbox_xmax,
                    ST_YMax(geom)                                               AS bbox_ymax
                FROM ST_Read('{mpa_shp}')
            ) TO '{mpa_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"  MPA saved {mpa_parquet}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    con.close()

if __name__ == "__main__":
    convert_to_parquet()