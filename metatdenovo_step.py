#!/usr/bin/env python3

import os
import time
import logging
from metatdenovo_funcs import send_json_message

def monitor_execution(monitor_execution_params_d: dict) -> None:
    analysis_path       = monitor_execution_params_d['analysis_path']
    send_message_script = monitor_execution_params_d['send_message_script']
    return_value        = monitor_execution_params_d['return_value']
    start_time          = monitor_execution_params_d['start_time']
    task_id             = monitor_execution_params_d['task_id']
    analysis_record_id  = monitor_execution_params_d['analysis_record_id']
    assembler           = monitor_execution_params_d['assembler']
    orf_caller          = monitor_execution_params_d['orf_caller']
    logging.info(monitor_execution_params_d)
    logging.info(analysis_path)
    os.chdir(analysis_path)
    step_dict = {
        'tTaskId'         : task_id,
        'analysisRecordId': analysis_record_id,
        'pipelineName'    : 'metatdenovo'.upper(),
        'analysisStatus'  : '',
        'startDate'       : start_time,
        'endDate'         : '',
        'error'           : 0,
        'taskName'        : 'Step'
    }

# step_flag    0  流程开始
# step_flag    1  数据预处理
# step_flag    2  转录本拼接+比对
# step_flag    3  ORF 检测
# step_flag    4  功能和分类注释
# step_flag    5  MultiQC报告
    
    # 流程开始
    step_flag = 0
    # send the message of step_0
    detect_num = 0
    execution_trace_file = ''
    while True:
        pipeline_info_dir = '{}/results/pipeline_info'.format(analysis_path)
        if os.path.exists(pipeline_info_dir):
            for file in sorted(os.listdir(pipeline_info_dir), key=lambda x: os.path.getmtime(os.path.join(pipeline_info_dir, x)), reverse=True):
                if file.startswith('execution_trace'):
                    execution_trace_file = os.path.join(pipeline_info_dir, file)
                    break
                else:
                    pass
        if detect_num > 5:
            break
        else:
            detect_num += 1
            time.sleep(60)
    step_dict['analysisStatus'] = '流程开始'
    step_file_name = 'step_start.json'
    if return_value == 0 and os.path.exists(execution_trace_file):
        step_dict['endDate'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        step_dict['error']   = 0
        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
        step_flag = 1
    else:
        step_dict['endDate'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        step_dict['error']   = 1
        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
        exit('metatdenovo startup failed')
    # send the message of step_x
    while step_flag < 6:
        # if cancel.txt or Cancel.txt is found, kill the pipeline and exit
        if os.path.exists('{}/cancel.txt'.format(analysis_path)) or os.path.exists('{}/Cancel.txt'.format(analysis_path)):
            step_dict['error'] = 2
            step_dict['endDate'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            if os.path.exists('{}/.nextflow.pid'.format(analysis_path)):
                with open('{}/.nextflow.pid'.format(analysis_path)) as f:
                    os.system('kill {}'.format(f.read().strip('/n')))
            send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
            exit('The analysis was cancelled')
        with open(execution_trace_file, encoding='UTF-8') as trace_f:
            for line in trace_f:
                # 数据预处理
                if step_flag == 1:
                    step_file_name = 'step_preprocessing.json'
                    if ('INPUT_CHECK:SAMPLESHEET_CHECK' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = '数据预处理'
                        continue
                    if ('MEGAHIT_INTERLEAVED' in line) or ('WRITESPADESYAML' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 2
                        break
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_quantification')
                # 转录本拼接+比对
                if step_flag == 2:
                    step_file_name = 'step_assembly.json'
                    if ('MEGAHIT' in line) or ('SPADES' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = '转录本拼接+比对'
                        continue
                    if ('PRODIGAL' in line) or ('PROKKA' in line) or ('TRANSDECODER' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 3
                        break
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_assembly')
                # ORF 检测
                if step_flag == 3:
                    step_file_name = 'step_orf_caller.json'
                    if ('PRODIGAL' in line) or ('PROKKA' in line) or ('TRANSDECODER' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = 'ORF 检测'
                        continue
                    if ('HMMCLASSIFY' in line) or ('EUKULELE' in line) or ('EGGNOG' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 4
                        break
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_orf_caller')
                # 功能和分类注释
                if step_flag == 4:
                    step_file_name = 'step_functional_and_taxonomical_annotation.json'
                    if ('HMMCLASSIFY' in line) or ('EUKULELE' in line) or ('EGGNOG' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = '功能和分类注释'
                        continue
                    if ('MERGE_TABLES' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 5
                        break
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_functional_and_taxonomical_annotation')
                # MultiQC报告
                if step_flag == 5:
                    step_file_name = 'step_multiqc.json'
                    if ('MERGE_TABLES' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = 'MultiQC报告'
                        continue
                    if ('MULTIQC' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 6
                        break
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_functional_and_taxonomical_annotation')
