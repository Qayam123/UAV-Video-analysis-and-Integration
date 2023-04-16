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
import datetime
# import pandas as pd
# import pygeohash as gh
import Geohash
from jproperties import Properties
import _ssl
import logging
from logging import FileHandler
from logging import Formatter
import socket
LOG_FORMAT = ('%(asctime)s - %(name)s - %(lineno)d - %(message)s')
LOG_LEVEL = logging.DEBUG
#Application logger
Application_LOG_FILE = "./ImsasLog/Application.log"

application_logger = logging.getLogger("Admin")
application_logger.setLevel(LOG_LEVEL)
application_logger_file_handler = FileHandler(Application_LOG_FILE)
application_logger_file_handler.setLevel(LOG_LEVEL)
application_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
application_logger.addHandler(application_logger_file_handler)

# Error logger
LOG_LEVEL2 = logging.ERROR
Error_LOG_FILE = "./ImsasLog/Error.log"
Error_logger = logging.getLogger("Admin")
Error_logger.setLevel(LOG_LEVEL2)
Error_file_handler = FileHandler(Error_LOG_FILE)
Error_file_handler.setLevel(LOG_LEVEL2)
Error_file_handler.setFormatter(Formatter(LOG_FORMAT))
Error_logger.addHandler(Error_file_handler)




def load_properties(prop_name):
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
    settings.BOOTSTRAP_SERVER = prop.get("BOOTSTRAP_SERVER").data
    settings.TOPIC_NAME_1 = prop.get("TOPIC_NAME_1").data
    settings.TOPIC_NAME_2 = prop.get("TOPIC_NAME_2").data
    settings.ML_MODEL_PATH = prop.get("ML_MODEL_PATH").data
    settings.MODEL_IOU = prop.get("MODEL_IOU").data
    settings.MODEL_CONF = prop.get("MODEL_CONF").data
    settings.DETECT_OBJECT = prop.get("DETECT_OBJECT").data
    settings.DISPLAY_CONSUMER_FRAME  = prop.get("DISPLAY_CONSUMER_FRAME").data

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
    load_properties('Uav_ml.properties')
    modelpath = settings.ML_MODEL_PATH
    videopath = settings.VIDEO_PATH_1
    urlpath = settings.STREAM_UDP_PATH_1
    topic_name_1 = settings.TOPIC_NAME_1
    topic_name_2 = settings.TOPIC_NAME_2
    print("[STARTED...]")
    # run(modelpath, videopath, topic_name_1)
    run(modelpath, urlpath, topic_name_2)
def tracktype():
    res = None
    k = "name"
    for i in data[0]['results_list']:
        if all(k in sub for sub in [i]):
            res = i[k]
            return res

def trackspeed(lat1, lon1, lat2, lon2):
    sp = None
    M_PI = 3.14159265

    lat1 = lat1 * M_PI / 180.0
    lon1 = lon1 * M_PI / 180.0
    lat2 = lat2 * M_PI / 180.0
    lon2 = lon2 * M_PI / 180.0
    # radius of earth in metres
    r = 6378100;
    #     // P
    rho1 = r * math.cos(lat1)
    z1 = r * math.sin(lat1);
    x1 = rho1 * math.cos(lon1)
    y1 = rho1 * math.sin(lon1)
    #     // Q
    rho2 = r * math.cos(lat2)
    z2 = r * math.sin(lat2)
    x2 = rho2 * math.cos(lon2)
    y2 = rho2 * math.sin(lon2)
    #     // Dot product
    dot = (x1 * x2 + y1 * y2 + z1 * z2)
    cos_theta = dot / (r * r)
    theta = math.acos(cos_theta)
    #     // Distance in Metres
    return r * theta

def bearing(lat1, lon1, lat2, lon2):
    # Convert coordinates to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Compute differences
    dLon = lon2 - lon1

    # Compute bearing using Haversine formula
    y = sin(dLon) * cos(lat2)
    x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dLon)
    bearing = degrees(atan2(y, x))

    # Normalize to range [0,360)
    return (bearing + 360) % 360

if __name__ == '__main__':
    load_properties('Uav_ml_consumer.properties')
    topic = settings.TOPIC_NAME_2

    consumer = KafkaConsumer(topic,bootstrap_servers=settings.BOOTSTRAP_SERVER)
    for msg in consumer:
        data = next(get_extracted_data(consumer))

        try:
            pointlist = data[1]['corner_points_latlon']
            s = (data[0].get("metadata_stream_2")[2][3])
            d = datetime.datetime.fromisoformat(s)
            d2 = datetime.datetime.fromisoformat(s)
            p1 = pointlist[0]
            p2 = pointlist[3]
            dist = trackspeed(p1[0],p1[1],p2[0],p2[1])
            direction = bearing(p1[0], p1[1], p2[0], p2[1])
            dist = dist / 1852.0  # to Nautical mile
            time_s = d.second
            # time_s = (data[0].get("metadata_stream_2")[2][3])
            speed_NMps = dist / time_s
            track_speed_Nm = (speed_NMps * 3600.0)
            application_logger.info("Data streaming")
        except:
            Error_logger.error('Missing Metadata')
            continue


        t = tracktype()
        temp = data[1]["tgt_loc"]
        count=0
        if len(temp) == 2:
            Track_tuple = [{}]
            # Track_name =
            Track_id = 'UAV'
            Latitude = temp[0]
            Longitude = temp[1]
            time_stamp = data[0].get("metadata_stream_2")[2][3]
            track_type = (data[0]['results_list'])
            uuid = gh.encode(temp[0], temp[1], precision=5)
            time_stamp = time.time()
            tz = datetime.timezone.utc
            ft = "%Y-%m-%dT%H:%M:%S%z"
            times = datetime.datetime.now().strftime(ft)
            epoch = parser.parse(times).timestamp()
            count += int('00') + 1
        
            trackname = f"UAV{uuid}{int(epoch % 100000)}{count}"
            json_value ={'latitude' : Latitude,'longitude': Longitude,'course' : direction,
                            'timeStamp':int(epoch),'speed': track_speed_Nm}
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost',5050))
            socketData = json.dumps(json_value).encode("utf-8")
            client.sendall(socketData)
 
