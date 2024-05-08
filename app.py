from flask import Flask, request, jsonify
from app.database import Neo4jConnection
from flask_cors import CORS
from flask import render_template


app = Flask(__name__)
CORS(app)
conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="12345678")

@app.route('/recommendations', methods=['GET'])
def recommendations():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "User ID is required"}), 400
    results = conn.get_recommendations(user_id)
    if results:
        return jsonify(results)
    else:
        return jsonify({"error": "No recommendations found"}), 404


@app.route('/path-analysis')
def path_analysis():
    result = conn.get_path_analysis()
    if result:
        return jsonify(result)
    else:
        return jsonify({"error": "No path analysis data available"}), 404

@app.route('/community-detection')
def community_detection():
    result = conn.get_community_detection()
    if result:
        return jsonify(result)
    else:
        return jsonify({"error": "No community detection data available"}), 404


@app.route('/centrality')
def centrality():
    result = conn.get_centrality()
    if result:
        return jsonify(result)
    else:
        return jsonify({"error": "No centrality data available"}), 404

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
