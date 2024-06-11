import SAR

if __name__ == "__main__":
    for i in range(7):
        if i < 1:
            continue
        filedir = f'/Users/israel/Development/TerraSAR-X/testFiles{i}'
        meta = SAR.get_img_metadata(filedir)
        out_file = SAR.gen_cropped_png(filedir, meta)
        SAR.add_annotations(out_file, meta)
        print(out_file)
