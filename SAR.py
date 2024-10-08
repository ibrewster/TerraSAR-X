import ftplib
import math
import os
import pickle
import re
import shutil
import tempfile
import tarfile
import urllib

import xml.etree.ElementTree as ET

from contextlib import contextmanager
from datetime import datetime

from io import BytesIO
from pathlib import Path

import cairosvg
import mattermostdriver
import psycopg
import pygmt
import svgutils

# import sharepy
from affine import Affine
from matplotlib import font_manager
from PIL import Image, ImageDraw, ImageFont

# Gmail API utils
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from osgeo import gdal, osr, gdalconst

osr.UseExceptions()  # Must be called immediately to avoid warnings.

from osgeo_utils.gdal2tiles import main as img2tiles

# for encoding/decoding messages in base64
from base64 import urlsafe_b64decode

import config

FILEDIR = os.path.dirname(__file__)


@contextmanager
def PostgresCursor(
    host='akutan.avo.alaska.edu', database='geodesy', user=None, password=None
) -> psycopg.Cursor:
    if user is None:
        user = getattr(config, "DB_USER", None)
    if password is None:
        password = getattr(config, "DB_PASS", None)

    conn = psycopg.connect(host=host, dbname=database, user=user, password=password)
    cursor = conn.cursor()

    yield cursor

    try:
        conn.rollback()
        conn.close()
    except Exception:
        pass


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


def mm_post_gif(meta, path, mattermost, channel_id, num=4, message=None):
    gen_date = datetime.today().strftime('%Y%m%d')
    filename = f"{meta['volc']}_orb_{meta['orbit']}_{meta['dir']}_{gen_date}.gif"
    with tempfile.TemporaryDirectory() as out_dir:
        gif = os.path.join(out_dir, filename)
        files = sorted(sorted(path.glob('*/*.png'), reverse=True)[:num])
        images = [Image.open(f) for f in files]
        images[0].save(gif, save_all=True, append_images=images[1:], duration=1000, loop=0)
        mm_upload(mattermost, channel_id, message, gif, filename)


def mm_post_image(meta, image, mattermost, channel_id):
    filename = f"{meta['volc']}_orb_{meta['orbit']}_{meta['dir']}_{meta['date'].strftime('%Y%m%d %H:%M')}.png"
    volcano = meta['volc']
    ftp_link = f"ftp://akutan.avo.alaska.edu/TerraSAR-X/zip/Orbit {meta['orbit']}-{meta['dir']}/{meta['date'].strftime('%Y%m%d')}/{meta['tgzName']}"
    ftp_link = urllib.parse.quote(ftp_link, safe='/:')
    geodesy_link = f"https://apps.avo.alaska.edu/geodesy/map?overlay=Orbit {meta['orbit']}-{meta['dir']}&type=amp&sensor=TerraSAR-X&dfrom={meta['date'].strftime('%Y-%m-%d')}"
    geodesy_link = urllib.parse.quote(geodesy_link, safe='/:?&=')

    matt_message = f"""### {volcano.title()} SAR image available
**Image Date:** {meta['date'].strftime('%m/%d/%Y')}
**ZIP Download:** [Click Here to download]({ftp_link})
**Web Link:** [View in web interface]({geodesy_link})"""
    mm_upload(mattermost, channel_id, matt_message, image, filename)


def mm_upload(mattermost, channel_id, message, image=None, img_name=None):
    post_payload = {
        "channel_id": channel_id,
    }

    # First, upload the thumbnail, if any
    if image and img_name:
        with open(image, "rb") as img:
            upload_result = mattermost.files.upload_file(
                channel_id=channel_id, files={"files": (img_name, img)}
            )

        matt_id = upload_result["file_infos"][0]["id"]
        post_payload["file_ids"] = [matt_id]

    if message:
        post_payload["message"] = message

    mattermost.posts.create_post(post_payload)


def file_message(service, message_id, success=True):
    print(f"Filing message with id: {message_id}")
    label_id = 'Label_3229944419067452259'
    if not success:
        label_id = 'Label_205232427347535884'
    modify_body = {
        "addLabelIds": [label_id],
        "removeLabelIds": ['UNREAD', 'INBOX'],
    }
    service.users().messages().modify(userId="me", id=message_id, body=modify_body).execute()


def get_messages(service):
    print("Retrieving messages")
    messages = search_messages(service, "from:Simon.Plank@dlr.de")

    url_pattern = re.compile(r"\n\s+(ftps:\/\/.+.tar.gz)")
    packages = []
    ids = []
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

        packages.append(download_url)
        ids.append(message_id)

    return (packages, ids)


def download_package(url):
    print("Downloading file:", url)
    url_breakdown = re.search(r"ftps:\/\/([^@]+)@([^\/]+)\/+([^\s]+.tar.gz)", url)
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
    return filedata, filename


def extract_files(file):
    print("Extracting downloaded file")
    tempdir = tempfile.TemporaryDirectory()
    img_pattern = re.compile(r"IMAGEDATA\/[^\s]+.tif")
    xml_pattern = re.compile(r"SAR.L1B\/[^\s\/]+\/[^\s\/]+.xml")
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

        shutil.move(img_path, os.path.join(tempdir.name, "sar_image.tif"))
        shutil.move(xml_path, os.path.join(tempdir.name, "metadata.xml"))

    return tempdir


def get_geoinfo(ds):
    wkt_string = ds.GetProjection()
    srs = osr.SpatialReference(wkt=wkt_string)

    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(4326)

    transform = osr.CoordinateTransformation(srs, dst_srs)

    projcs = srs.GetAttrValue("projcs")
    utm_zone = projcs.split("/")[1].replace("UTM zone", "").strip()

    return transform, utm_zone


def create_png(file_dir, meta):
    print("Processing image")
    gdal.DontUseExceptions()

    img_file = os.path.join(file_dir, "sar_image.tif")
    aux_xml_file = img_file + ".aux.xml"
    if os.path.isfile(aux_xml_file):
        os.unlink(aux_xml_file)

    clean_file, gmt_region = gen_clean_png(file_dir)
    cropped_file = gen_cropped_png(file_dir, meta)

    return cropped_file, clean_file, gmt_region


def gen_clean_png(file_dir):
    img_file = os.path.join(file_dir, "sar_image.tif")
    clean_file = os.path.join(file_dir, "sar_image_clean.tiff")

    gdal.AllRegister()
    gdal.UseExceptions()
    ds = gdal.Open(img_file)

    transform, utm_zone = get_geoinfo(ds)

    ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()

    lrx = ulx + (ds.RasterXSize * xres)
    lry = uly + (ds.RasterYSize * yres)  # yres is negitive

    region = [ulx, lry, lrx, uly]

    # 21 is the magic number recommended by the documentation. I have no idea.
    minLat, minLon, maxLat, maxLon = transform.TransformBounds(*region, 21)
    gmt_region = [minLon, maxLon, minLat, maxLat]

    png_opts = {
        "outputType": gdalconst.GDT_Byte,  # 0-255
        "noData": 0,
    }

    mem_file_path = "/vsimem/scaled_image.tiff"
    gdal.Translate(mem_file_path, ds, **png_opts)

    kwargs = {
        "dstSRS": 'EPSG:3857',
        "multithread": True,
        "warpOptions": ["NUM_THREADS=ALL_CPUS"],
        "creationOptions": ["NUM_THREADS=ALL_CPUS"],
    }

    gdal.Warp(clean_file, mem_file_path, **kwargs)

    ds = None
    gdal.Unlink(mem_file_path)

    tile_dir = os.path.join(file_dir, 'mapTiles')

    cpu_count = os.cpu_count()
    tiles_argv = [
        'SAR.py',
        '-z',
        '10-17',
        '-w',
        'none',
        f'--processes={cpu_count}',
        clean_file,
        tile_dir,
    ]

    try:
        img2tiles(tiles_argv, called_from_main=True)
    except TypeError:
        __spec__ = None
        img2tiles(tiles_argv)

    shutil.copy(img_file, tile_dir)

    return tile_dir, gmt_region


def gen_cropped_png(file_dir, meta):
    img_file = os.path.join(file_dir, "sar_image.tif")

    filename = f"{meta['volc']}_orb_{meta['orbit']}_{meta['dir']}.png"

    out_file = os.path.join(file_dir, filename)
    cropped_file = os.path.join(file_dir, "sar_image_cropped.tif")

    gdal.AllRegister()
    ds = gdal.Open(img_file)

    transform, utm_zone = get_geoinfo(ds)

    try:
        half_side = meta['size'] / 2
    except KeyError:
        # If we have no size specified, use the entire image
        gt = ds.GetGeoTransform()
        x_min = gt[0]
        y_max = gt[3]
        x_max = x_min + gt[1] * ds.RasterXSize
        y_min = y_max + gt[5] * ds.RasterYSize

        meta['size'] = x_max - x_min

        proj_cropped_bounds = [x_min, y_max, x_max, y_min]
    else:
        proj_cropped_bounds = [
            meta['centerx'] - half_side,
            meta['centery'] + half_side,
            meta['centerx'] + half_side,
            meta['centery'] - half_side,
        ]

    minLatC, minLonC, maxLatC, maxLonC = transform.TransformBounds(*proj_cropped_bounds, 21)
    gmt_cropped_region = [minLonC, maxLonC, minLatC, maxLatC]

    if meta['rotation'] != 0:
        ds = rotate_dataset(ds, meta['rotation'], (meta['centerx'], meta['centery']))

    # Re-project the image to lat/lon so it plays nicely with pygmt
    gdal.Warp(
        cropped_file,
        ds,
        multithread=True,
        warpOptions=['NUM_THREADS=ALL_CPUS'],
        creationOptions=['NUM_THREADS=ALL_CPUS'],
        dstSRS="+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +over +center_lon=-180",
    )

    ds = None

    cropped_inch_width = 6

    scale_length = meta['size'] / 5  # Length of the scale bar in meters, 1/5 the length of the side
    if scale_length < 1000:
        scale_length = round(scale_length / 50) * 50
        scale_unit = "e"
    else:
        scale_length = round(scale_length / 1000) * 1000
        scale_length /= 1000
        scale_unit = "k"

    # Possible approach: round to either 500 or 1000, depending on which is closest to original value.
    # If value is closer to 1000, round to the nearest 1000, else round to the nearest 500
    # nearest_1000 = round(scale_length / 1000) * 1000
    # nearest_500 = round(scale_length / 500) * 500
    # if abs(scale_length - nearest_1000) < abs(scale_length - nearest_500):
    # return nearest_1000
    # else:
    # return nearest_500

    # cropped_projection = f"U{utm_zone}/{cropped_inch_width}i"
    cropped_projection = f"M{cropped_inch_width}i"

    fig = pygmt.Figure()

    frame = ["WSen+gblack"]
    pygmt.makecpt(cmap="gray", series=[0, 300])

    if not meta['zoomed']:
        frame.append("a")

    with pygmt.config(
        MAP_ANNOT_OBLIQUE="lat_parallel", MAP_FRAME_TYPE="plain", FORMAT_GEO_MAP="ddd.xxF"
    ):
        fig.basemap(projection=cropped_projection, region=gmt_cropped_region, frame=frame)

    fig.grdimage(cropped_file, dpi=300, nan_transparent="black")

    with pygmt.config(
        FONT_LABEL="12p,black",
        FONT_ANNOT_PRIMARY="12p,black",
        MAP_TICK_PEN_PRIMARY="1p,black",
    ):

        fig.basemap(map_scale=f"jLB+w{scale_length}{scale_unit}+o0.224i/0.2i")

    with pygmt.config(
        FONT_LABEL="12p,white",
        FONT_ANNOT_PRIMARY="12p,white",
        MAP_TICK_PEN_PRIMARY="1p,white",
    ):

        fig.basemap(map_scale=f"jLB+w{scale_length}{scale_unit}+o0.212i")

    fig.savefig(out_file, transparent=False)

    return out_file


def rotate_dataset(ds, angle, point):
    gt = ds.GetGeoTransform()

    meters_to_rotx = point[0] - gt[0]
    meters_to_roty = point[1] - gt[3]  # Will be negitive, but it cancels out correctly.

    xrot = meters_to_rotx / gt[1]
    yrot = meters_to_roty / gt[5]

    pivot = (xrot, yrot)

    affine_src = Affine.from_gdal(*gt)
    affine_dest = affine_src * affine_src.rotation(angle, pivot)
    new_gt = affine_dest.to_gdal()

    ds.SetGeoTransform(new_gt)

    return ds


def add_north(png_file, meta, margin):
    script_dir = os.path.dirname(__file__)
    svg_file = os.path.join(script_dir, 'NorthArrow.svg')
    svg = svgutils.transform.fromfile(svg_file)
    angle = math.radians(meta['rotation'])

    cur_width = float(svg.width)
    cur_height = float(svg.height)

    transformed_svg = svg.getroot()

    # Start by rotating around the center
    transformed_svg.rotate(meta['rotation'], cur_width / 2, cur_height / 2)

    # calculate the bounding box size for the rotated image
    rotated_width = cur_width * abs(math.cos(angle)) + cur_height * abs(math.sin(angle))
    rotated_height = cur_width * abs(math.sin(angle)) + cur_height * abs(math.cos(angle))

    #  Figure out the shift needed to place the rotated figure back at 0,0
    width_diff = rotated_width - cur_width
    height_diff = rotated_height - cur_height

    x_shift = width_diff / 2
    y_shift = height_diff / 2

    transformed_svg.moveto(x_shift, y_shift)

    # Calculate the scaling factor to produce a good size on the final image
    img_width = png_file.size[0]
    arrow_width = img_width / 10  # one-tenth of the original width is reasonable.

    # Calculate the scale factor needed to obtain the desired width, and scale the SVG
    scale_factor = arrow_width / rotated_width
    transformed_svg.scale(scale_factor)

    # Calculate the new size after scaling.
    scaled_width = rotated_width * scale_factor
    scaled_height = rotated_height * scale_factor

    fig = svgutils.transform.SVGFigure(scaled_width, scaled_height)
    fig.append(transformed_svg)
    fig.set_size((f"{scaled_width}", f"{scaled_height}"))

    svg_data = BytesIO(fig.to_str())
    svg_data.seek(0)

    # convert to a PNG
    png_data = cairosvg.svg2png(file_obj=svg_data)
    north = Image.open(BytesIO(png_data))

    # and add it to the image
    png_file.paste(north, (margin, margin), north.convert("RGBA"))


def add_annotations(png_file, meta):
    print("Adding Annotations")
    volcano = meta['volc']
    mission = meta['mission']
    timestamp = meta['date'].strftime('%Y-%m-%d %H:%M')

    margin = 24
    img = Image.open(png_file)
    img_width, img_height = img.size
    draw = ImageDraw.Draw(img)

    title = f"""{volcano} {mission}
{timestamp} UTC"""

    font_file = font_manager.findfont("helvetica")
    font_size = 8  # start with a minimum size
    text_width_target = img.size[0] / 3  # One-third image width.

    # technically gives a font producing a size slightly larger than desired,
    # but only by one font size so not quibbling.
    while True:
        font = ImageFont.truetype(font_file, font_size)
        left, top, right, bottom = draw.multiline_textbbox((0, 0), title, font=font)
        text_width = right - left
        if text_width >= text_width_target:
            break

        font_size += 1

    print("Using font size of", font_size)

    text_left = img_width - text_width - margin
    text_top = margin

    txt_left_s = text_left + 2
    txt_top_s = text_top + 2

    draw.text((txt_left_s, txt_top_s), title, (0, 0, 0), font=font, align="right")
    draw.text((text_left, text_top), title, (255, 255, 255), font=font, align="right")

    font = ImageFont.truetype(font_file, round(font_size / 1.6))

    cur_year = datetime.today().year
    copyright_str = f"""TerraSAR-X/TanDEM-X
© DLR e.V.{cur_year}"""

    left, top, right, bottom = draw.multiline_textbbox((0, 0), copyright_str, font=font)
    cp_width = right - left
    cp_height = bottom - top
    cp_left = img_width - cp_width - margin

    # Add DLR Logo
    margin_right = 15
    if not meta['zoomed']:
        margin += 50
        margin_right = 25
        cp_left -= 10

    logo_file = os.path.join(os.path.dirname(__file__), "DLRlogo.png")
    logo = Image.open(logo_file)
    logo_w, logo_h = logo.size
    if logo_h > cp_height:
        logo_top = img_height - logo_h - margin
        cp_top = logo_top
    else:
        cp_top = img_height - cp_height - margin
        logo_top = cp_top

    logo_left = cp_left - logo_w - margin_right

    img.paste(logo, (logo_left, logo_top), logo.convert("RGBA"))

    shadow_left = cp_left + 2
    shadow_top = cp_top + 2
    draw.text((shadow_left, shadow_top), copyright_str, (0, 0, 0), font=font)
    draw.text((cp_left, cp_top), copyright_str, (255, 255, 255), font=font)

    if meta['rotation'] != 0:
        add_north(img, meta, margin)

    img.save(png_file)


mission_lookup = {
    'TDX-1': 'TanDEM-X',
    'TSX-1': 'TerraSAR-X',
}


def get_img_metadata(file_dir):
    meta = {}

    #  Load XML meta
    tree = ET.parse(os.path.join(file_dir, 'metadata.xml'))
    root = tree.getroot()

    #  Pull some data about the mission that produced this image
    mission_info = root.find('productInfo/missionInfo')
    orbit = mission_info.find('relOrbit').text
    direction = mission_info.find('orbitDirection').text
    mission = mission_info.find('mission').text
    print(mission)
    mission_name = mission_lookup.get(mission, 'TerraSAR-X')
    meta['mission'] = mission_name
    meta['orbit'] = orbit
    meta['dir'] = "ASC" if direction == "ASCENDING" else "DESC"
    order_name = root.find('setup/orderInfo/userData/customerOrderName').text
    order_id = root.find('setup/orderInfo/userData/customerOrderID').text
    customer_num = os.path.commonprefix(
        [order_name, order_id]
    )  # this could probably be hardcoded, but I'm paranoid.

    order_name = order_name.replace(customer_num, '')

    order_date = re.search(r"\d{8}", order_name).group(0)
    order_search = order_name.replace(order_date, 'YYYYMMDD')

    scene_date = root.find('productInfo/sceneInfo/start/timeUTC').text
    scene_date = datetime.strptime(scene_date, '%Y-%m-%dT%H:%M:%S.%fZ')
    meta['date'] = scene_date

    # Image file name
    img_name = root.find('productComponents/imageData/file/location/filename').text
    meta['imgName'] = img_name

    meta_sql = """SELECT
        volcano_name,
        targetx,
        targety,
        side,
        rotation,
        notes ILIKE '%%zoomed%%' as zoomed
    FROM tsx
    INNER JOIN volcano
    ON volcano.volcano_id=tsx.volcano
    WHERE ordername=%s;"""
    with PostgresCursor() as cursor:
        cursor.execute(meta_sql, (order_search,))
        db_meta = cursor.fetchone()

    if db_meta is not None:
        meta['volc'] = db_meta[0]
        meta['centerx'] = db_meta[1]
        meta['centery'] = db_meta[2]
        meta['size'] = db_meta[3]
        meta['rotation'] = db_meta[4]
        meta['zoomed'] = db_meta[5]
    else:
        # raise FileNotFoundError("Unable to load image parameters")
        print("****WARNING: order not found in database. Using fallback parameters")
        volc = order_name.split("_")[0]
        meta['volc'] = volc
        meta['rotation'] = 0
        meta['zoomed'] = False

    return meta


def main():
    top_dir = Path(config.KML_DIR)
    archive_dir = Path(config.ARCHIVE_DIR)
    cropped_archive = archive_dir / 'cropped'
    zip_archive = archive_dir / 'zip'

    service = gmail_authenticate()
    packages, ids = get_messages(service)
    mattermost, channel_id = connect_to_mattermost()

    for url, message_id in zip(packages, ids):
        try:
            tar_gz_file, tar_gz_filename = download_package(url)
            file_dir = extract_files(tar_gz_file)
        except FileNotFoundError:
            file_message(service, message_id, success=False)
            continue

        try:
            meta = get_img_metadata(file_dir.name)
        except FileNotFoundError:
            print("Unable to get metadata for message")
            file_message(service, message_id, success=False)
            continue

        meta['tgzName'] = tar_gz_filename
        volc = meta['volc']

        orbit_dir = Path(f"Orbit {meta['orbit']}-{meta['dir']}")
        dest_dir_str = orbit_dir / meta['date'].strftime('%Y%m%d')

        # Archive the zip file
        zip_dir = zip_archive / dest_dir_str
        os.makedirs(zip_dir, exist_ok=True)
        zip_file = zip_dir / tar_gz_filename
        tar_gz_file.seek(0)
        with open(zip_file, 'wb') as f:
            f.write(tar_gz_file.read())

        annotated_file, tile_dir, png_region = create_png(file_dir.name, meta)

        # Place the tile directory in the web directory for serving
        web_dir = top_dir / dest_dir_str
        os.makedirs(web_dir, exist_ok=True)

        tile_dest: Path = web_dir / Path(meta['imgName']).stem

        if tile_dest.is_dir():
            shutil.rmtree(str(tile_dest))

        shutil.move(tile_dir, tile_dest)

        add_annotations(annotated_file, meta)

        # Archive the annotated file
        crop_dir = cropped_archive / dest_dir_str
        os.makedirs(crop_dir, exist_ok=True)
        shutil.copy(annotated_file, crop_dir)

        mm_post_image(meta, annotated_file, mattermost, channel_id)

        gif_source = cropped_archive / orbit_dir
        mm_post_gif(meta, gif_source, mattermost, channel_id)

        file_message(service, message_id)
        print("Completed processing imagery for", volc)
    print("All messages processed.")


if __name__ == "__main__":
    ########### DEBUG ############
    # file_dir = 'testFiles5'
    # meta = get_img_metadata(file_dir)
    # clean_file, png_region = gen_clean_png(file_dir)
    # kml_dir = top_dir / f"Orbit {meta['orbit']}-{meta['dir']}" / meta['date'].strftime('%Y%m%d')
    # os.makedirs(kml_dir, exist_ok=True)

    # kmz_dest = kml_dir / kmz_file.name
    # if kmz_dest.is_file():
    # kmz_dest.unlink()

    # shutil.move(kmz_file, kml_dir)
    # exit(0)
    ##############################

    main()
