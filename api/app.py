from datetime import datetime, timezone
import logging
from flask import Flask, jsonify, request
from kafka_utils import latest_reading, publish_reading
from lake_utils import daily_stats, recent_anomalies, sensor_types
app=Flask(__name__); app.logger.setLevel(logging.INFO)
VALID={"temperature","humidity","pressure"}; UNITS={"temperature":"C","humidity":"%","pressure":"hPa"}
def ok(data=None,status=200,**extra):
    body={"status":"success","data":data}; body.update(extra); return jsonify(body),status
def fail(message,status=400,**extra):
    body={"status":"error","code":status,"message":message}; body.update(extra); return jsonify(body),status
def validate(sensor):
    return fail(f"Unknown sensor type '{sensor}'.",404) if sensor not in VALID else None
@app.route("/api/v1/health")
def health(): return jsonify({"status":"ok","service":"aerosense-api","timestamp":datetime.now(timezone.utc).isoformat()}),200
@app.route("/api/v1/sensors")
def sensors():
    data=sensor_types(); return ok(data,count=len(data))
@app.route("/api/v1/sensors/<sensor_type>/latest")
def latest(sensor_type):
    err=validate(sensor_type)
    if err: return err
    try:
        reading=latest_reading(sensor_type)
        return ok(reading) if reading else fail(f"No readings found for sensor '{sensor_type}'.",404)
    except Exception as exc:
        app.logger.exception("latest failed: %s",exc); return fail("Could not read latest Kafka event.",500)
@app.route("/api/v1/sensors/<sensor_type>/stats")
def stats(sensor_type):
    err=validate(sensor_type)
    if err: return err
    try:
        days=int(request.args.get("days","7"))
        if days<1 or days>90: return fail("'days' must be between 1 and 90.",400)
    except ValueError: return fail("'days' must be an integer.",400)
    try:
        data=daily_stats(sensor_type,days); return ok(data,sensor_type=sensor_type,days=days,count=len(data))
    except Exception as exc:
        app.logger.exception("stats failed: %s",exc); return fail("Could not read Parquet statistics.",500)
@app.route("/api/v1/anomalies")
def anomalies():
    sensor=request.args.get("sensor"); limit_raw=request.args.get("limit","20")
    if sensor and sensor not in VALID: return fail(f"Unknown sensor type '{sensor}'.",400)
    try:
        limit=int(limit_raw)
        if limit<1 or limit>200: return fail("'limit' must be between 1 and 200.",400)
    except ValueError: return fail("'limit' must be an integer.",400)
    try:
        data=recent_anomalies(sensor_type=sensor,limit=limit); return ok(data,count=len(data))
    except Exception as exc:
        app.logger.exception("anomalies failed: %s",exc); return fail("Could not read anomaly data.",500)
@app.route("/api/v1/readings",methods=["POST"])
def create_reading():
    body=request.get_json(silent=True)
    if body is None: return fail("Request body must be valid JSON.",400)
    missing={"sensor","value"}-set(body.keys())
    if missing: return fail(f"Missing required field(s): {sorted(missing)}",400)
    sensor=body["sensor"]
    if sensor not in VALID: return fail(f"Invalid sensor type '{sensor}'.",422)
    try: value=float(body["value"])
    except (ValueError,TypeError): return fail("'value' must be numeric.",422)
    anomaly=(sensor=="temperature" and value>35) or (sensor=="humidity" and value>90) or (sensor=="pressure" and (value<990 or value>1030))
    reading={"sensor":sensor,"value":value,"unit":body.get("unit",UNITS[sensor]),"timestamp":int(datetime.now(timezone.utc).timestamp()*1000),"source":body.get("source","api"),"anomaly":anomaly}
    try:
        meta=publish_reading(reading); return ok({"reading":reading,"kafka":meta},status=201,message="Reading published.")
    except Exception as exc:
        app.logger.exception("create failed: %s",exc); return fail("Could not publish reading to Kafka.",500)
@app.errorhandler(404)
def not_found(_): return fail("The requested resource was not found.",404)
@app.errorhandler(405)
def method_not_allowed(_): return fail("HTTP method not allowed for this endpoint.",405)
@app.errorhandler(500)
def internal_error(_): return fail("Internal server error.",500)
if __name__=="__main__": app.run(host="0.0.0.0",port=5000,debug=True)
