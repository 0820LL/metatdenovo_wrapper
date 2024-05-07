#!/usr/bin/bash

if [ "$#" -ne 1 ]; then
    echo "The script need exactly 1 argument: config.json"
    ehco "/.../$0 /.../config.json"
    exit
fi

config_file="$1"
analysis_dir=$(dirname "$config_file")
script_path=$(dirname "$(realpath "$0")")
configuration_file="$script_path"/../../config/configuration.json
sendMessage=$(jq ".jms" "$configuration_file" | sed 's/\"//g')

cd "$analysis_dir" || exit
python3 "$script_path"/metatdenovo_start.py \
    --cfg "$config_file" \
    --metatdenovo_path "$script_path"/../nf-core-metatdenovo_1.0.1/1_0_1/main.nf \
    --send_message_script "$sendMessage"
