from kafka import KafkaConsumer, TopicPartition
import socket
from kafka.admin import KafkaAdminClient, NewTopic
import cv2
from geopy import distance
import pyproj
from vector3d.vector import Vector
import av
import imutils
import yolov5
import klvdata
import settings
from app_code.producer import stream_udp_event, list_kafka_topics
from app_code.consumer import get_extracted_data
import app_code.lat_lon_coords
import pika
import json
import time
import requests
from _collections import OrderedDict
import math
import calendar
import pygeohash as gh
from jproperties import Properties
import Geohash
from math import radians, degrees, atan2, sin, cos, sqrt
from numpy import arctan2,random, sin, cos
from dateutil import parser
import logging
from logging import FileHandler
from logging import Formatter
import os
import scipy
from scipy import signal
import numpy as np
import haversine as hs
from haversine import Unit
import pandas as pd
import time
import kafka
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import cv2
from geopy import distance
import pyproj
from vector3d.vector import Vector
import av
import imutils
import yolov5
import klvdata
import settings
from app_code.producer import stream_udp_event, list_kafka_topics
from app_code.consumer import get_extracted_data
import app_code.lat_lon_coords
import pika
import json
# import config as cfg
from subprocess import call
import requests
from _collections import OrderedDict
import math
# import pandas as pd
# import pygeohash as gh
import Geohash
from jproperties import Properties
import _ssl
import threading
import time
import datetime
from datetime import datetime


log_dir = './ImsasLog'

# Manage Log Files
for fl_nm in os.listdir(log_dir):
    fl_pth = os.path.join(log_dir, fl_nm)
    with open(fl_pth, 'r') as f:
        frst_ln = f.readline()
    datetime_str = frst_ln.split('  ')[0]
    crtd_dt = datetime.now()
    day_diff = (datetime.today().date() - crtd_dt.date()).days

    if day_diff > 7:
        os.remove(fl_pth)
    else:
        if day_diff > 0:
            if fl_nm in ['Application.log', 'Error.log']:
                new_fl_nm = f'{fl_nm.replace(".log", "")}-{crtd_dt:%a}.log'
                new_fl_pth = os.path.join(log_dir, new_fl_nm)
                os.rename(fl_pth, new_fl_pth)

app_log_path = os.path.join(log_dir, 'Application.log')
err_log_path = os.path.join(log_dir, 'Error.log')

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

formatter = logging.Formatter('%(asctime)s  %(levelname)s: %(filename)s - %(lineno)d : %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

logger_app = logging.getLogger('AdminAPP')
handler_app = logging.FileHandler(app_log_path, mode='a')
handler_app.setFormatter(formatter)
logger_app.addHandler(handler_app)
logger_app.setLevel(logging.INFO)

logger_err = logging.getLogger('AdminERR')
handler_err = logging.FileHandler(err_log_path, mode="a")
handler_err.setFormatter(formatter)
logger_err.addHandler(handler_err)
logger_err.setLevel(logging.INFO)

def load_properties1(prop_name):
    prop = Properties()
    with open(prop_name, 'rb') as read_prop:
        prop.load(read_prop)

    settings.BOOTSTRAP_SERVER = prop.get("BOOTSTRAP_SERVER").data
    settings.STREAM_UDP_PATH_1 = prop.get("STREAM_UDP_PATH_1").data
    settings.TOPIC_NAME_1 = prop.get("TOPIC_NAME_1").data
    settings.TOPIC_NAME_2 = prop.get("TOPIC_NAME_2").data
    # settings.DISPLAY_PRODUCER_FRAME = prop.get("DISPLAY_PRODUCER_FRAME").data
    settings.CLASSES_LIST = prop.get("CLASSES_LIST").data
    settings.FRAME_WIDTH = prop.get("FRAME_WIDTH").data
    settings.ML_MODEL_PATH = prop.get("ML_MODEL_PATH").data


def connect_kafka_producer_objdet():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER,retries=5)
        logger1.info("Video is getting relayed")
        
    except Exception as ex:
        print(str(ex))
        logger2.error("Kafka not connected")
    finally:
        return _producer


def run(modelpath, videopath, topic_name):
    detection_producer = connect_kafka_producer_objdet()
    print("[STARTED...]")
    stream_udp_event(detection_producer, modelpath, videopath, topic_name)
    logger1.info("yolov5 model running")


def uav_ml():
     # topics = list_kafka_topics(settings.BOOTSTRAP_SERVER)
     # print(topics)
    load_properties1('Uav_ml.properties')
    modelpath = settings.ML_MODEL_PATH
    videopath = settings.VIDEO_PATH_1
    urlpath = settings.STREAM_UDP_PATH_1
    topic_name_1 = settings.TOPIC_NAME_1
    topic_name_2 = settings.TOPIC_NAME_2
    print("[STARTED...]")
    # run(modelpath, videopath, topic_name_1)
    run(modelpath, urlpath, topic_name_2)

pr_time=time.time()
def load_properties2(prop_name):
    prop = Properties()
    with open(prop_name, 'rb') as read_prop:
        prop.load(read_prop)

    settings.BOOTSTRAP_SERVER = prop.get("BOOTSTRAP_SERVER").data
    settings.TOPIC_NAME_1 = prop.get("TOPIC_NAME_1").data
    settings.TOPIC_NAME_2 = prop.get("TOPIC_NAME_2").data
    settings.ML_MODEL_PATH = prop.get("ML_MODEL_PATH").data
    settings.MODEL_IOU = prop.get("MODEL_IOU").data
    settings.MODEL_CONF = prop.get("MODEL_CONF").data
    settings.DETECT_OBJECT = prop.get("DETECT_OBJECT").data
    settings.DISPLAY_CONSUMER_FRAME = prop.get("DISPLAY_CONSUMER_FRAME").data


def tracktype():
    res = None
    k = "name"
    for i in data[0]['results_list']:
        if all(k in sub for sub in [i]):
            res = i[k]
            return res


def distance(lat1, lon1, lat2, lon2):
    loc1 = (lat1, lon1)
    loc2 = (lat2, lon2)
    dist1 = hs.haversine(loc1, loc2)
    return dist1

def compute_course(lat1, lon1, lat2, lon2):
    """Calculate course using two points"""

    # Convert coordinates to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Compute initial bearing
    dLon = lon2 - lon1
    y = sin(dLon) * cos(lat2)
    x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dLon)
    bearing = degrees(atan2(y, x))
    init_bearing = (bearing + 360) % 360
    print(init_bearing)

    # Compute Final bearing
    dLon = lon1 - lon2
    y = sin(dLon) * cos(lat2)
    x = cos(lat2) * sin(lat1) - sin(lat2) * cos(lat1) * cos(dLon)
    bearing = degrees(atan2(y, x))
    bearing = (bearing + 360) % 360
    final_bearng = (bearing + 180) % 360
    print(final_bearng)

    # Taking the difference
    course = np.round((final_bearng - init_bearing), 4)
    return course

def uav_ml_consumer():
    load_properties2('Uav_ml_consumer.properties')
    topic = settings.TOPIC_NAME_2
    consumer = KafkaConsumer(topic, bootstrap_servers=settings.BOOTSTRAP_SERVER)

    dist = 0
    flag = 0
    prevLat = 0
    prevLon = 0
    count = 0
    # a = {lat,lon}
    # b = {lat,lon}
    for msg in consumer:
        try:
            data = next(get_extracted_data(consumer))
            lat_long = data[1]["tgt_loc"]
            if len(lat_long) == 2:
                if prevLat == 0 and prevLon ==0:
                    prevLat = lat_long[0]
                    prevLon = lat_long[1]
                    startTime = datetime.now()
                    continue
                currLat = lat_long[0]
                currLon = lat_long[1]

                #Get Distance
                d = distance(prevLat, prevLon, currLat, currLon)
                dist = dist + d

                #Get Course
                course = compute_course(prevLat, prevLon, currLat, currLon)

                prevLat = currLat
                prevLon = currLon

                Latitude = np.round(currLat, 6)
                Longitude = np.round(currLon, 6)
                uuid = gh.encode(Latitude, Longitude, precision=5)
                ft = "%Y-%m-%dT%H:%M:%S%z"
                times = datetime.now().strftime(ft)
                epoch = parser.parse(times).timestamp()
                count += int('00') + 1
                trackname = f"UAV{uuid}{int(epoch % 100000)}{count}"
                Track_id = 'UAV'
                diffTime = datetime.now() - startTime
                print("diffTime",diffTime)
                if diffTime.total_seconds() >= 60:
                    speed = np.round((1944*dist)/diffTime.total_seconds(),3)
                    print("DISTANCE", np.round(dist * 1000, 6))
                    print("Course",course)
                    print("Vessel_speed in kts", speed)
                    prevLat = 0
                    prevLon = 0
                    dist = 0
                    print("OLD JSON OLD JSON OLD JSON OLD JSON OLD JSON OLD JSON OLD JSON OLD JSON OLD JSON")
                    json_value = {"messageType": "UAV1", "isNewTrack": "0", 'trackId': f"UAV{uuid}",
                                  'trackName': trackname,
                                  'latitude': Latitude, 'longitude': Longitude, 'course': course,
                                  'timeStamp': int(epoch), 'speed': speed, "height": "0",
                                  "sensorDatetime": int(epoch),
                                  'trackType': "CARGO", "trackQuality": "1",
                                  "header": "$PRTRK", "anomalyStatus": 'true', "status": "0",
                                  "anomalyCodeList": [], "isT0Track": 'false', "iffFlag": "U"}
                    logger_app.info("Track Tuple generated")

                    try:
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect(('localhost', 5051))
                        socketData = json.dumps(json_value).encode("utf-8")
                        client.sendall(socketData)
                        logger_app.info("Result data sent")
                    except:
                        logger_err.error("Web socket server not connected")

                else:
                    if np.round(currLat,4) - np.round(prevLat,4) > 0.0001 or np.round(currLon,4) - np.round(prevLon,4) > 0.0001:
                        print("NEW JSON NEW JSON NEW JSON NEW JSON NEW JSON NEW JSON NEW JSON NEW JSON NEW JSON")
                        json_value = {"messageType": "UAV1", "isNewTrack": "0", 'trackId': f"UAV{uuid}",
                                  'trackName': trackname,
                                  'latitude': Latitude, 'longitude': Longitude,
                                  'timeStamp': int(epoch), "height": "0",
                                  "sensorDatetime": int(epoch),
                                  'trackType': "CARGO", "trackQuality": "1",
                                  "header": "$PRTRK", "anomalyStatus": 'true', "status": "0",
                                  "anomalyCodeList": [], "isT0Track": 'false', "iffFlag": "U"}
                        logger_app.info("Track Tuple generated")

                        try:
                            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            client.connect(('localhost', 5051))
                            socketData = json.dumps(json_value).encode("utf-8")
                            client.sendall(socketData)
                            logger_app.info("Result data sent")
                        except:
                            logger_err.error("Web socket server not connected")

                     
        except:
            continue

if __name__ == "__main__":
    
    t1=threading.Thread(target=uav_ml)
    t2=threading.Thread(target=uav_ml_consumer)
    t1.start()
    time.sleep(10)
    t2.start()
    t1.join()
    t2.join()






