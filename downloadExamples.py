import xml.etree.cElementTree as ET
import boto3
from pymongo import MongoClient
from PIL import Image
import os
import numpy as np
from tqdm import tqdm

def save_annotation(filename_text, path_text, image_shape, objects, save_filename):
    annotation = ET.Element('annotation')
    folder = ET.SubElement(annotation, 'folder')
    folder.text = 'train'

    filename = ET.SubElement(annotation, 'filename')
    filename.text = filename_text

    path = ET.SubElement(annotation, 'path')
    path.text = path_text

    source = ET.SubElement(annotation, 'source')
    database = ET.SubElement(source, 'database')
    database.text = 'Unknown'

    size = ET.SubElement(annotation, 'size')
    width = ET.SubElement(size, 'width')
    height = ET.SubElement(size, 'height')
    depth = ET.SubElement(size, 'depth')
    width.text = str(image_shape[0])
    height.text = str(image_shape[1])
    depth.text = str(image_shape[2])

    segmented = ET.SubElement(annotation, 'segmented')
    segmented.text = '0'

    for obj in objects:
        obj_tag = ET.SubElement(annotation, 'object')
        name = ET.SubElement(obj_tag, 'name')
        pose = ET.SubElement(obj_tag, 'pose')
        truncated = ET.SubElement(obj_tag, 'truncated')
        difficult = ET.SubElement(obj_tag, 'difficult')

        name.text = obj['name'] # Tag Name
        pose.text = 'Unspecified'
        truncated.text = '1'
        difficult.text = '0'

        bbox = ET.SubElement(obj_tag, 'bndbox')
        xmin = ET.SubElement(bbox, 'xmin')
        ymin = ET.SubElement(bbox, 'ymin')
        xmax = ET.SubElement(bbox, 'xmax')
        ymax = ET.SubElement(bbox, 'ymax')

        xmin.text = obj['xmin']
        xmax.text = obj['xmax']
        ymin.text = obj['ymin']
        ymax.text = obj['ymax']

    ET.ElementTree(annotation).write(save_filename, encoding='UTF-8')


access_key = input('AWS access key:')
secret_key = input('AWS access secret:')
session = boto3.Session(access_key, secret_key)
s3 = session.resource('s3')
bucket = s3.Bucket('finlab-bucket')

mongo_username = input('Mongo Username:')
mongo_password = input('Mongo Password:')
mongo = MongoClient(f'mongodb://{mongo_username}:{mongo_password}@royceschultz.com:27017/?authSource=users&readPreference=primary&appname=MongoDB%20Compass&ssl=false')

SAVE_DIR = 'images'
CURRENT_PATH = os.getcwd()

# Make directiry if not exists
if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

# Ask user how many images to download
n = None
while n is None:
    n = input('How many?:')
    try:
        n = int(n)
    except:
        print('please enter an int')
        n = None

pipeline = [{
  '$sample': {
    'size': n
  }
}]
for header in tqdm(mongo.finlab_beta.MoodysGoldHeaders.aggregate(pipeline), total=n):
    obj = bucket.Object(header['_id'])
    res = obj.get()
    im = Image.open(res['Body']).convert('RGB')

    image_path = os.path.join(CURRENT_PATH, SAVE_DIR, header['_id'])

    if os.path.exists(image_path):
        print('already downloaded image, skipping...')
        continue

    pre, ext = os.path.splitext(header['_id'])
    save_path = os.path.join(SAVE_DIR, pre + '.xml')
    try:
        objects = [{
            'name': 'company',
            'xmin': str(int(h['raw_origin'][0])),
            'xmax': str(int(h['raw_origin'][0] + h['raw_delta'][0])),
            'ymin': str(int(h['raw_origin'][1])),
            'ymax': str(int(h['raw_origin'][1] + h['raw_delta'][1])),
        } for h in header['headers']]
        save_annotation(filename_text=header['_id'], path_text=image_path, image_shape=np.asarray(im).shape, objects=objects, save_filename=save_path)
        im.save(os.path.join(SAVE_DIR, header['_id']))
    except Exception as e:
        print('Exception: ', e, ', continuing...')
