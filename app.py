from flask import Flask, jsonify, request, render_template
from scripts.searoutes import (
    get_water_bodies, search_water_bodies,
    get_countries, search_countries,
    get_ports, search_ports,
    calculate_full_route
)
from scripts.disasters import get_disasters_for_route
from scripts.chokepoints import get_chokepoints_on_route

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("homepage.html")

@app.route("/route_planner")
def route_planner():
    return render_template("index.html")

@app.route("/api/water-bodies")
def water_bodies():
    try:
        return jsonify(get_water_bodies())
    except Exception as e:
        print(f"Error fetching water bodies: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/search/water-bodies")
def search_wb():
    q = request.args.get("q", "")
    try:
        return jsonify(search_water_bodies(q))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/countries")
def countries():
    water_body = request.args.get("water_body")
    if not water_body:
        return jsonify({"error": "water_body is required"}), 400
    try:
        return jsonify(get_countries(water_body))
    except Exception as e:
        print(f"Error fetching countries: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/search/countries")
def search_c():
    q = request.args.get("q", "")
    water_body = request.args.get("water_body", "")
    try:
        return jsonify(search_countries(q, water_body))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ports")
def ports():
    water_body = request.args.get("water_body")
    country_code = request.args.get("country_code")
    if not water_body or not country_code:
        return jsonify({"error": "water_body and country_code are required"}), 400
    try:
        return jsonify(get_ports(water_body, country_code))
    except Exception as e:
        print(f"Error fetching ports: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/search/ports")
def search_p():
    q = request.args.get("q", "")
    water_body = request.args.get("water_body", "")
    country_code = request.args.get("country_code", "")
    try:
        return jsonify(search_ports(q, water_body, country_code))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/route", methods=["POST"])
def route():
    data = request.get_json()
    stops = data.get("stops", [])

    if len(stops) < 2:
        return jsonify({"error": "At least 2 stops are required"}), 400

    for i in range(len(stops) - 1):
        if (stops[i]["port_name"] == stops[i+1]["port_name"] and
                stops[i]["country_code"] == stops[i+1]["country_code"]):
            return jsonify({"error": f"Consecutive duplicate port at stop {i+1} and {i+2}"}), 400

    try:
        result = calculate_full_route(stops)
        if not result:
            return jsonify({"error": "Route calculation failed"}), 500

        # Collect all route coordinates across all segments
        all_route_coords = []
        for seg in result["segments"]:
            all_route_coords.extend(seg["coordinates"])

        # Find chokepoints near the route
        chokepoints = get_chokepoints_on_route(all_route_coords)
        result["chokepoints"] = chokepoints

        return jsonify(result)
    except Exception as e:
        print(f"Error calculating route: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/disasters", methods=["POST"])
def disasters():
    data = request.get_json()
    segments = data.get("segments", [])
    if not segments:
        return jsonify({"error": "segments are required"}), 400
    try:
        result = get_disasters_for_route(segments)
        return jsonify(result)
    except Exception as e:
        print(f"Error fetching disasters: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)