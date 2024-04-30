#!/bin/bash

/apps/TerraSAR-X/tsx-env/bin/python /apps/TerraSAR-X/SAR.py
rsync -r /geodesy/data/TerraSAR-X/archive/zip/ root@akutan.avo.alaska.edu:/ftp_home/TerraSAR-X/
rm -r /geodesy/data/TerraSAR-X/archive/zip/*
