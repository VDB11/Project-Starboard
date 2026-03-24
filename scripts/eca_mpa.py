import duckdb
import json
import os

class FastECAMPA:
    def __init__(self):
        self.con = None
        self.loaded = False

    def load_data(self):
        if self.loaded:
            return

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "Project-Starboard"))
        data_folder = os.path.join(project_root, "Data")
        db_file = os.path.join(data_folder, "eca_mpa.duckdb")

        if not os.path.exists(db_file):
            print(f"ERROR: DuckDB not found at {db_file}. Run build_duckdb.py first.")
            self.loaded = False
            return

        try:
            self.con = duckdb.connect(database=db_file, read_only=True)
            self.con.execute("LOAD spatial;")
            self.loaded = True
            print("ECA/MPA data loaded from DuckDB successfully.")
        except Exception as e:
            print(f"Failed to load ECA/MPA data: {e}")
            self.loaded = False

    def check_route_intersections(self, route_coordinates):
        """
        route_coordinates : list of [lat, lon] pairs
        Returns           : list of dicts with type, name, geometry + metadata
        """
        if not self.loaded or not route_coordinates or len(route_coordinates) < 2:
            return []

        try:
            # Build the route WKT (lon lat order)
            coords_str = ", ".join(f"{lon} {lat}" for lat, lon in route_coordinates)
            route_wkt = f"LINESTRING({coords_str})"

            # Bounding box pre-filter
            lons = [lon for _, lon in route_coordinates]
            lats = [lat for lat, _ in route_coordinates]
            route_xmin, route_xmax = min(lons), max(lons)
            route_ymin, route_ymax = min(lats), max(lats)

            rows = self.con.execute("""
                SELECT
                    type, name, regulation, designation, iucn_cat, status,
                    status_yr, gov_type, iso3, marine_area_km2, no_take,
                    ST_AsGeoJSON(geom) AS geojson
                FROM eca_mpa
                WHERE
                    bbox_xmax >= ? AND bbox_xmin <= ?
                    AND bbox_ymax >= ? AND bbox_ymin <= ?
                    AND ST_Intersects(geom, ST_GeomFromText(?))
            """, [
                route_xmin, route_xmax,
                route_ymin, route_ymax,
                route_wkt
            ]).fetchall()

            results = []
            for row in rows:
                geojson_geom = json.loads(row[11])
                results.append({
                    "type": row[0],
                    "name": str(row[1]).replace("_", " ").strip(),
                    "regulation": row[2] or "",
                    "designation": row[3] or "",
                    "iucn_cat": row[4] or "",
                    "status": row[5] or "",
                    "status_yr": row[6] or "",
                    "gov_type": row[7] or "",
                    "iso3": row[8] or "",
                    "marine_area_km2": row[9] or "",
                    "no_take": row[10] or "",
                    "geometry": geojson_geom,
                })
            return results

        except Exception as e:
            print(f"ECA/MPA intersection check failed: {e}")
            return []


fast_eca_mpa = FastECAMPA()