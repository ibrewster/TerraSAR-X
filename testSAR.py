import os
import shutil

from pathlib import Path

from PIL import Image

import SAR, config

if __name__ == "__main__":
    archive_dir = Path(config.ARCHIVE_DIR)
    cropped_archive = archive_dir / 'cropped'
    
    files = cropped_archive / 'Orbit 146-DESC'

    meta = {
        'volc': 'Random',
        'orbit': '146',
        'dir': 'DESC',
    }
    
    mattermost, channel_id = SAR.connect_to_mattermost()
    
    SAR.mm_post_gif(meta, files, mattermost, channel_id)
    

    exit(0)
    # The below code processes the raw zip files.
    files = Path('/Users/israel/Downloads/TerraSAR Images/Orbit 146-DESC').glob('*/*.tar.gz')
    for file in files:
        with open(file, 'rb') as tgzfile:
            tempdir = SAR.extract_files(tgzfile)
            
        meta = SAR.get_img_metadata(tempdir.name)
        
        dest_dir_str = Path(f"Orbit {meta['orbit']}-{meta['dir']}") / meta['date'].strftime(
            '%Y%m%d'
        )
        crop_dir = cropped_archive / dest_dir_str
        
        out_file = SAR.gen_cropped_png(tempdir.name, meta)
        SAR.add_annotations(out_file, meta)
        
        os.makedirs(crop_dir, exist_ok=True)
        shutil.copy(out_file, crop_dir)
        print(crop_dir)
