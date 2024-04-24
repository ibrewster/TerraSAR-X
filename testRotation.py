from osgeo import gdal, osr
import pygmt
import math
import os

from affine import Affine


gdal.UseExceptions()
os.environ['GDAL_PAM_ENABLED'] = 'NO'

source = '/Users/israel/Development/TerraSAR-X/testFiles4/sar_image.tif'
dest = '/Users/israel/Development/TerraSAR-X/testFiles4/sar_image_cropped.tif'
dest_rotated = '/Users/israel/Development/TerraSAR-X/testFiles4/sar_image_rotated.tif'

rotation = -110
size = 4000
centerx = 567943
centery = 6067874

half_side = size / 2

small_crop = [centerx - half_side, centery + half_side, centerx + half_side, centery - half_side]

ds = gdal.Open(source)

# Get translation from original coordinates to lat/lon
wkt_string = ds.GetProjection()
srs = osr.SpatialReference(wkt=wkt_string)

dst_srs = osr.SpatialReference()
dst_srs.ImportFromEPSG(4326)

transform = osr.CoordinateTransformation(srs, dst_srs)

minLat, minLon, maxLat, maxLon = transform.TransformBounds(*small_crop, 21)
gmt_region = [minLon, maxLon, minLat, maxLat]

# Apply the rotation to the image
gt = ds.GetGeoTransform()

meters_to_rotx = centerx - gt[0]
meters_to_roty = centery - gt[3]  # Will be negitive, but it cancels out correctly.

xrot = meters_to_rotx / gt[1]
yrot = meters_to_roty / gt[5]
pivot = (xrot, yrot)

affine_src = Affine.from_gdal(*gt)
affine_dest = affine_src * affine_src.rotation(rotation, pivot)
new_gt = affine_dest.to_gdal()

ds.SetGeoTransform(new_gt)

ds = gdal.Warp(
    dest,
    ds,
    outputBounds=small_crop,
    multithread=True,
    warpOptions=['NUM_THREADS=ALL_CPUS'],
    creationOptions=['NUM_THREADS=ALL_CPUS'],
    # dstSRS="+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +over +lon_wrap=-180",
)

# Convert the image to lat/lon so it works nicely with pygmt
gdal.Warp(
    dest,
    ds,
    multithread=True,
    warpOptions=['NUM_THREADS=ALL_CPUS'],
    creationOptions=['NUM_THREADS=ALL_CPUS'],
    dstSRS="+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +over +lon_wrap=-180",
)


pygmt.makecpt(cmap="gray", series=[0, 300])
fig = pygmt.Figure()

fig.grdimage(
    dest,
    dpi=300,
    nan_transparent="black",
    projection='U3N/8i',
    region=gmt_region,
)

csl = 1000
with pygmt.config(
    FONT_LABEL="12p,black",
    FONT_ANNOT_PRIMARY="12p,black",
    MAP_TICK_PEN_PRIMARY="1p,black",
):

    fig.basemap(map_scale=f"jLB+w{csl}e+o0.224i/0.2i")

with pygmt.config(
    FONT_LABEL="12p,white",
    FONT_ANNOT_PRIMARY="12p,white",
    MAP_TICK_PEN_PRIMARY="1p,white",
):

    fig.basemap(map_scale=f"jLB+w{csl}e+o0.212i")

fig.show()
