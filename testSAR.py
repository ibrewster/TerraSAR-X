from pathlib import Path

import SAR, config

if __name__ == "__main__":
    archive_dir = Path(config.ARCHIVE_DIR)
    cropped_archive = archive_dir / 'cropped'
    
    files = Path('/Users/israel/Downloads/TerraSAR Images/Orbit 139-ASC').glob('*/*.tar.gz')
    print(files)
    for file in files:
        tempdir = SAR.extract_files(file)
        meta = SAR.get_img_metadata(tempdir.name)
        out_file = SAR.gen_cropped_png(tempdir.name, meta)
        SAR.add_annotations(out_file, meta)
        print(out_file)
