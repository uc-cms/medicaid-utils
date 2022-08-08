#!/bin/bash

new_state_name="TS2"

# grab CT directory structure, since it has all files: (ref: https://www.linuxshelltips.com/copy-directory-structure-linux/)
tree -dfi --noreport /mnt/data2/medicaid/2016/CT | xargs -I{} mkdir -p ../{}

# bit of a hack to fix the error created by above method (DEFINITELY DO NOT RUN THIS ANYWHERE NEAR ROOT)
mv ../mnt/data2/medicaid/2016/CT ../${new_state_name}
rm -r ../mnt

# CT doesn't have demc in the de section for some reason
mkdir -p ../${new_state_name}/taf/de/demc/csv ../${new_state_name}/taf/de/demc/parquet