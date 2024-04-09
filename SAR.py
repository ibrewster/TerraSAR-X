import ftplib
import os
import pickle
import re
import shutil
import tempfile
import tarfile
import zipfile

from datetime import datetime
from io import BytesIO
from pathlib import Path

import mattermostdriver
import pygmt
import requests

# import sharepy

from matplotlib import font_manager
from PIL import Image, ImageDraw, ImageFont

# Gmail API utils
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from osgeo import gdal, osr

# for encoding/decoding messages in base64
from base64 import urlsafe_b64decode

import config

FILEDIR = os.path.dirname(__file__)


def connect_to_mattermost():
    mattermost = mattermostdriver.Driver(
        {
            "url": config.MATTERMOST_URL,
            "token": config.MATTERMOST_TOKEN,
            "port": config.MATTERMOST_PORT,
        }
    )

    mattermost.login()
    channel_id = mattermost.channels.get_channel_by_name_and_team_name(
        config.MATTERMOST_TEAM, config.MATTERMOST_CHANNEL
    )["id"]
    return (mattermost, channel_id)


def gmail_authenticate():
    SCOPES = ["https://mail.google.com/"]
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    token_path = os.path.join(FILEDIR, "token.pickle")
    if os.path.exists(token_path):
        with open(token_path, "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            creds_path = os.path.join(FILEDIR, "credentials.json")
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open(token_path, "wb") as token:
            pickle.dump(creds, token)
    return build("gmail", "v1", credentials=creds)


def search_messages(service, query):
    result = service.users().messages().list(userId="me", q=query, labelIds=["INBOX"]).execute()
    messages = []
    if "messages" in result:
        messages.extend(result["messages"])
    while "nextPageToken" in result:
        page_token = result["nextPageToken"]
        result = (
            service.users().messages().list(userId="me", q=query, pageToken=page_token).execute()
        )
        if "messages" in result:
            messages.extend(result["messages"])
    return messages


def upload_to_mattermost(filename, image, volcano, mattermost, channel_id):

    filename = f"{filename}.png"

    matt_message = f"""### {volcano.title()} SAR image available"""
    post_payload = {
        "channel_id": channel_id,
    }

    # First, upload the thumbnail, if any
    with open(image, "rb") as img:
        upload_result = mattermost.files.upload_file(
            channel_id=channel_id, files={"files": (filename, img)}
        )

    matt_id = upload_result["file_infos"][0]["id"]
    post_payload["file_ids"] = [matt_id]
    post_payload["message"] = matt_message

    mattermost.posts.create_post(post_payload)


def file_message(service, message_id):
    print(f"Filing message with id: {message_id}")
    modify_body = {
        "addLabelIds": ['Label_3229944419067452259'],
        "removeLabelIds": ['UNREAD', 'INBOX'],
    }
    service.users().messages().modify(userId="me", id=message_id, body=modify_body).execute()


def get_messages():
    print("Retrieving messages")
    service = gmail_authenticate()
    messages = search_messages(service, "from:Simon.Plank@dlr.de")

    url_pattern = re.compile("\n\s+(ftps:\/\/.+.tar.gz)")
    packages = []
    print(f"{len(messages)} messages found")
    for message in messages:
        message_id = message["id"]
        msg = service.users().messages().get(userId="me", id=message_id, format="full").execute()
        if msg["payload"]["body"]["size"] > 0:
            body = urlsafe_b64decode(msg["payload"]["body"]["data"]).decode()
        elif (
            "parts" in msg["payload"]
            and msg["payload"]["parts"][0].get(
                "body",
                {
                    "size": 0,
                },
            )["size"]
            > 0
        ):
            body = urlsafe_b64decode(msg["payload"]["parts"][0]["body"]["data"]).decode()
        try:
            download_url = url_pattern.search(body).group(1)
        except AttributeError:
            print("Unable to parse URL from body. Skipping")
            continue
        name_prefix = download_url.split("@")[0].replace("ftps://", "")
        name_pattern = f"[nN]ame\s=\s{name_prefix}_([^\s]+)"
        name_match = re.search(name_pattern, body)
        package_name = name_match.group(1)
        volc = package_name.split("_")[0]
        packages.append((download_url, volc))
        file_message(service, message_id)

    return packages


def download_package(url):
    print("Downloading file:", url)
    url_breakdown = re.search("ftps:\/\/([^@]+)@([^\/]+)\/+([^\s]+.tar.gz)", url)
    user = url_breakdown.group(1)
    server = url_breakdown.group(2)
    filename = url_breakdown.group(3)

    filedata = BytesIO()
    ftps_server = ftplib.FTP_TLS(server, user, config.FTP_PASSWORD)
    ftps_server.prot_p()
    try:
        ftps_server.retrbinary(f"RETR {filename}", filedata.write)
    except ftplib.error_perm:
        print("Unable to access file")
        raise FileNotFoundError("Unable to access file")

    print("Downloaded file", filename)

    filedata.seek(0)  # go back to the begining for reading
    return filedata


def extract_files(file):
    print("Extracting downloaded file")
    tempdir = tempfile.TemporaryDirectory()
    img_pattern = re.compile("IMAGEDATA\/[^\s]+.tif")
    xml_pattern = re.compile("SAR.L1B\/[^\s\/]+\/[^\s\/]+.xml")
    with tarfile.open(fileobj=file, mode="r") as tf, tempfile.TemporaryDirectory() as td:
        files = tf.getnames()
        tf.extractall(td)

        try:
            img_file = next((x for x in files if img_pattern.search(x)))
        except StopIteration:
            print("Image file not found in archive")
            raise FileNotFoundError("No Image file found")
        xml_file = next((x for x in files if xml_pattern.search(x)))

        img_path = os.path.join(td, img_file)
        xml_path = os.path.join(td, xml_file)

        img_name = os.path.basename(img_path)
        xml_name = os.path.basename(xml_path)

        date_parts = re.search("_(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})", xml_name).groups()
        img_date = f"{'-'.join(date_parts[:3])} {':'.join(date_parts[-2:])}"

        shutil.move(img_path, os.path.join(tempdir.name, "sar_image.tif"))
        shutil.move(xml_path, os.path.join(tempdir.name, "metadata.xml"))

    return (tempdir, img_name, img_date)


def create_png(file_dir):
    print("Processing image")
    gdal.AllRegister()
    img_file = os.path.join(file_dir, "sar_image.tif")
    out_file = os.path.join(file_dir, "sar_image.png")
    warped_file = os.path.join(file_dir, "sar_image_warped.tif")
    ds = gdal.Open(img_file)

    wkt_string = ds.GetProjection()
    srs = osr.SpatialReference(wkt=wkt_string)
    projcs = srs.GetAttrValue("projcs")
    utm_zone = projcs.split("/")[1].replace("UTM zone", "").strip()

    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(4326)

    transform = osr.CoordinateTransformation(srs, dst_srs)

    ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()

    lrx = ulx + (ds.RasterXSize * xres)
    lry = uly + (ds.RasterYSize * yres)  # yres is negitive

    ds = None

    region = [ulx, lry, lrx, uly]
    minLat, minLon, maxLat, maxLon = transform.TransformBounds(*region, 21)
    gmt_region = [minLon, maxLon, minLat, maxLat]

    kwargs = {
        "dstSRS": "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +over +lon_wrap=-180",
        "multithread": True,
        "warpOptions": ["NUM_THREADS=ALL_CPUS"],
        "creationOptions": ["NUM_THREADS=ALL_CPUS"],
    }

    gdal.Warp(warped_file, img_file, **kwargs)

    fig = pygmt.Figure()

    projection = f"U{utm_zone}/7.65i"
    with pygmt.config(
        FONT_LABEL="12p,white",
        FONT_ANNOT_PRIMARY="12p,white",
        MAP_TICK_PEN_PRIMARY="1p,white",
    ):
        pygmt.makecpt(cmap="gray", series=[0, 300])

        fig.grdimage(
            warped_file, projection=projection, region=gmt_region, dpi=300, nan_transparent="black"
        )

        # fig.basemap(map_scale="jLB+w1+o0.612i")
        fig.savefig(out_file, transparent=True)

    return out_file, gmt_region


def add_annotations(png_file, volcano, timestamp):
    print("Adding Annotations")
    margin = 24
    img = Image.open(png_file)
    img_width, img_height = img.size
    draw = ImageDraw.Draw(img)

    title = f"""{volcano} TerraSAR-X
{timestamp} UTC"""

    font_file = font_manager.findfont("helvetica")
    font = ImageFont.truetype(font_file, 60)
    left, top, right, bottom = draw.multiline_textbbox((0, 0), title, font=font)
    text_width = right - left

    text_left = img_width - text_width - margin
    text_top = margin

    draw.text((text_left, text_top), title, (255, 255, 255), font=font)

    font = ImageFont.truetype(font_file, 30)

    cur_year = datetime.today().year
    copywrite_str = f"""TerraSAR-X/TanDEM-X
© DLR e.V.{cur_year}"""

    left, top, right, bottom = draw.multiline_textbbox((0, 0), copywrite_str, font=font)
    cp_width = right - left
    cp_height = bottom - top
    cp_left = img_width - cp_width - margin

    # Add DLR Logo
    logo_file = os.path.join(os.path.dirname(__file__), "DLRlogo.png")
    logo = Image.open(logo_file)
    logo_w, logo_h = logo.size
    if logo_h > cp_height:
        logo_top = img_height - logo_h - margin
        cp_top = logo_top
    else:
        cp_top = img_height - cp_height - margin
        logo_top = cp_top

    logo_left = cp_left - logo_w - 15
    img.paste(logo, (logo_left, logo_top), logo.convert("RGBA"))

    draw.text((cp_left, cp_top), copywrite_str, (128, 128, 128), font=font)

    img.save(png_file)


def sharepoint_upload(file, volcano):
    api_url = "https://doimspp.sharepoint.com/sites/GS-VSCAVOall/_api/web"
    list_url = f"getfolderbyserverrelativeurl('/sites/GS-VSCAVOall/Shared%20Documents/AVOfileshare/GEOPHYSICS/SAR/{volcano}')/Files"
    request_url = f"{api_url}/{list_url}"

    server = sharepy.connect(
        "doimspp.sharepoint.com", username=config.shareUSER, password=config.sharePASS
    )
    resp = server.get(request_url)
    print(resp.status_code)

def gen_kmz(file, img_name, img_date, bounds):
    file = Path(file)
    kml_template = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<GroundOverlay>
    <name>{file}</name>
    <Icon>
        <href>{file}</href>
    </Icon>
    <LatLonBox>
        <north>{north}</north>
        <east>{east}</east>
        <south>{south}</south>
        <west>{west}</west>
    </LatLonBox>
</GroundOverlay>
</kml>"""
    kmz_file = datetime.strptime(img_date, '%Y-%m-%d %H:%M').strftime('%Y%m%dT%H%M%S.kmz')
    kmz_name = file.parent / kmz_file
    west, east, south, north = bounds
    kml = kml_template.format(file=img_name, west=west, east=east, south=south, north=north)
    kml = kml.encode('UTF-8')
    with zipfile.ZipFile(kmz_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(str(file), img_name)
        zipf.writestr("doc.kml", kml)

    return kmz_name


if __name__ == "__main__":
    # from requests_html import HTMLSession
    # client_id = '00000003-0000-0ff1-ce00-000000000000'
    # tennant_id = '0693b5ba-4b18-4d7b-9341-f32f400a5494'
    # session = HTMLSession()
    # resp = session.get(url)
    # sharepoint_upload(None, 'Cleveland')

    # png_file = create_png('testFiles')
    # add_annotations('testFiles/sar_image.png', 'Cleveland', '2023-03-16 12:56')

    packages = get_messages()
    top_dir = Path(config.KML_DIR)
    mattermost, channel_id = connect_to_mattermost()

    for url, volc in packages:
        # url = 'ftps://PlankS_GEO3593_5@download.dsda.dlr.de//dims_op_oc_dfd2_693027697_1.tar.gz'
        # volc = 'Shishaldin'
        try:
            tar_gz_file = download_package(url)
            file_dir, img_name, img_date = extract_files(tar_gz_file)
        except FileNotFoundError:
            continue

        png_file, png_region = create_png(file_dir.name)
        kmz_file = gen_kmz(png_file, img_name, img_date, png_region)
        kml_dir = (
            top_dir / volc / datetime.strptime(img_date, '%Y-%m-%d %H:%M').strftime('%Y%m%dT%H%M%S')
        )
        os.makedirs(kml_dir, exist_ok=True)

        shutil.move(kmz_file, kml_dir)
        add_annotations(png_file, volc, img_date)

        # upload_to_mattermost(img_name.replace('.tif', ''), png_file, volc, mattermost, channel_id)
        print("Completed processing imagery for", volc)
    print("All messages processed.")
