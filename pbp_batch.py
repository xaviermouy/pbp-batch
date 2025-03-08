
import sys
import pbp.main_meta_generator
import pbp.main_hmb_generator
import yaml
import os
from pathlib import Path
from datetime import datetime, timedelta

def format_path(path):
    p = Path(path)
    # Convert to Windows-style path
    return str(p.as_posix()).replace('/', '\\') if os.name == 'nt' else str(p)

def write_pbp_glabalAttributes_file(config):
    yaml_template = {
        "title": "",
        "Summary": "",
        "Conventions": "",
        "keywords": "",
        "keywords_vocabulary": "",
        "history": "",
        "source": "",
        "acknowledgement": "",
        "license": "",
        "standard_name_vocabulary": "",
        "date_created": "",
        "creator_name": "",
        "creator_email": "",
        "creator_url": "",
        "institution": "",
        "publisher_name": "",
        "publisher_email": "",
        "publisher_url": "",
        "geospatial_bounds": "",
        "comment": "",
        "time_coverage_start": "",
        "time_coverage_end": "",
        "time_coverage_duration": "",
        "time_coverage_resolution": "",
        "platform": "",
        "instrument": "",
        "cdm_data_type": "",
        "references": ""
    }
    # Update template with values from current yaml file
    yaml_template.update({key: config[key] for key in yaml_template if key in config})
    yaml_template.update({key: config['pbp_job_agent'][key] for key in yaml_template if key in config['pbp_job_agent']})

    # write GlobalAttribute file for pbp
    orig_file_name = config['pbp_job_agent']['variable_attrs_orig']
    output_file = os.path.join(os.path.dirname(orig_file_name),'globalAttributes_pbp.yaml')
    with open(output_file, "w") as file:
        yaml.dump(yaml_template, file, default_flow_style=False, sort_keys=False)
    if os.name == "nt":
        output_file = "file:\\\\\\" + output_file
    if os.name == "posix":
        output_file = "file:///" + output_file
    return output_file

    print(f"Generated YAML saved to: {output_file}")

def load_yaml_file(yaml_file):
    with open(yaml_file, "r") as file:
        config = yaml.safe_load(file)
    # Reformat paths to linux/windows and add the URI format (e.g. file:///) when needed
    config['pbp_job_agent']['variable_attrs_orig'] = config['pbp_job_agent']['global_attrs']
    if os.name == "nt":
        config['pbp_job_agent']['audio_base_dir'] = "file:\\\\\\" + format_path(config['pbp_job_agent']['audio_base_dir'])
        config['pbp_job_agent']['global_attrs'] = "file:\\\\\\" + format_path(config['pbp_job_agent']['global_attrs'])
        config['pbp_job_agent']['variable_attrs'] = "file:\\\\\\" + format_path(config['pbp_job_agent']['variable_attrs'])
    if os.name == "posix":
        config['pbp_job_agent']['audio_base_dir'] = "file:///" + format_path(config['pbp_job_agent']['audio_base_dir'])
        config['pbp_job_agent']['global_attrs'] = "file:///" + format_path(config['pbp_job_agent']['global_attrs'])
        config['pbp_job_agent']['variable_attrs'] = "file:///" + format_path(config['pbp_job_agent']['variable_attrs'])

    config['pbp_job_agent']['json_base_dir'] = format_path(config['pbp_job_agent']['json_base_dir'])
    config['pbp_job_agent']['xml_dir'] = format_path(config['pbp_job_agent']['xml_dir'])
    config['pbp_job_agent']['nc_output_dir'] = format_path(config['pbp_job_agent']['nc_output_dir'])
    config['pbp_job_agent']['meta_output_dir'] = format_path(config['pbp_job_agent']['meta_output_dir'])
    config['pbp_job_agent']['log_dir'] = format_path(config['pbp_job_agent']['log_dir'])
    if config['pbp_job_agent']['sensitivity_uri'] == '':
        config['pbp_job_agent']['sensitivity_uri'] = None
    if config['pbp_job_agent']['sensitivity_flat_value'] == '':
        config['pbp_job_agent']['sensitivity_flat_value'] = None
    if config['pbp_job_agent']['voltage_multiplier'] == '':
        config['pbp_job_agent']['voltage_multiplier'] = None
    config['pbp_job_agent']['output_prefix'] = config['pbp_job_agent']['output_prefix'] if config['pbp_job_agent']['output_prefix'].endswith('_') else s + '_'
    config['pbp_job_agent']['subset_to'] = [int(num) for num in config['pbp_job_agent']['subset_to'].split()]
    return config

def run_pbp_meta_gen(recorder=None,uri=None,output_dir=None,json_base_dir=None,xml_dir=None,start=None,end=None,prefix=None):
    # Simulate command-line arguments
    sys.argv = [
        "main_meta_generator.py",
        "--recorder", recorder,
        "--uri", uri,
        "--output-dir", output_dir,
        "--json-base-dir", json_base_dir,
        "--xml-dir", xml_dir,
        "--start", start,
        "--end", end,
        "--prefix", prefix
    ]
    # Call main function
    pbp.main_meta_generator.main()

def run_pbp_hmd_gen(json_base_dir=None,audio_base_dir=None,date=None,output_dir=None,prefix=None,sensitivity_uri=None,sensitivity_flat_value=None,voltage_multiplier=None,subset_to=None,global_attrs=None,variable_attrs=None):
    # Simulate command-line arguments
    sys.argv = [
        "main_hmd_generator.py",
        "--json-base-dir", json_base_dir,
        "--audio-base-dir", audio_base_dir,
        "--date", date,
        "--output-dir", output_dir,
        "--output-prefix", prefix,
        "--global-attrs", global_attrs,
        "--variable-attrs", variable_attrs,
    ]
    # add subset
    tmp = ["--subset-to"]
    for freq in subset_to:
        tmp.append(str(freq))
    sys.argv = sys.argv + tmp
    # add optional argument if needed
    if sensitivity_flat_value is not None:
        sys.argv = sys.argv + ["--sensitivity-flat-value", sensitivity_flat_value]
    if sensitivity_uri is not None:
        sys.argv = sys.argv + ["--sensitivity-uri", sensitivity_uri]
    if voltage_multiplier is not None:
        sys.argv = sys.argv + ["--voltage-multiplier", voltage_multiplier]
    # Call main function
    pbp.main_hmb_generator.main()

def run_pbp_hmd_gen_batch(json_base_dir=None,audio_base_dir=None,start=None,end=None,output_dir=None,prefix=None,sensitivity_uri=None,sensitivity_flat_value=None,voltage_multiplier=None,subset_to=None,global_attrs=None,variable_attrs=None):
    # loop through each day of the deployment
    date_format = "%Y%m%d"
    delta = timedelta(days=1)
    start_date = datetime.strptime(start, date_format)
    end_date = datetime.strptime(end, date_format)
    # iterate over range of dates
    while (start_date <= end_date):
        date_str = start_date.strftime(date_format)
        print(date_str)
        run_pbp_hmd_gen(
            json_base_dir=json_base_dir,
            audio_base_dir=audio_base_dir,
            date=date_str,
            output_dir=output_dir,
            prefix=prefix,
            sensitivity_uri=sensitivity_uri,
            sensitivity_flat_value=sensitivity_flat_value,
            voltage_multiplier=voltage_multiplier,
            subset_to=subset_to,
            global_attrs=global_attrs,
            variable_attrs=variable_attrs
        )
        start_date += delta

def main():
    # load config parameters from YAML file
    yaml_file = r"C:\Users\xavier.mouy\Documents\Projects\2025_Galapagos\processing_outputs\WHOI_Galapagos_202305_Caseta\6478\pypam\META\globalAttributes_WHOI_Galapagos_202305_Caseta.yaml"
    config = load_yaml_file(yaml_file)

    # create GlobalAttribute file that PBP understands
    new_global_attrs_filename = write_pbp_glabalAttributes_file(config)

    # Generate meta data
    run_pbp_meta_gen(
        recorder=config['pbp_job_agent']['recorder'],
        uri=config['pbp_job_agent']['audio_base_dir'],
        output_dir=config['pbp_job_agent']['meta_output_dir'],
        json_base_dir=config['pbp_job_agent']['json_base_dir'],
        xml_dir=config['pbp_job_agent']['xml_dir'],
        start=config['pbp_job_agent']['start'],
        end=config['pbp_job_agent']['end'],
        prefix=config['pbp_job_agent']['prefix'],
    )

    # Calculate HMD
    run_pbp_hmd_gen_batch(
        json_base_dir=config['pbp_job_agent']['json_base_dir'],
        audio_base_dir=config['pbp_job_agent']['audio_base_dir'],
        start=config['pbp_job_agent']['start'],
        end=config['pbp_job_agent']['end'],
        output_dir=config['pbp_job_agent']['nc_output_dir'],
        prefix=config['pbp_job_agent']['output_prefix'],
        sensitivity_uri=config['pbp_job_agent']['sensitivity_uri'],
        sensitivity_flat_value = config['pbp_job_agent']['sensitivity_flat_value'],
        voltage_multiplier = config['pbp_job_agent']['voltage_multiplier'],
        subset_to=config['pbp_job_agent']['subset_to'],
        global_attrs=new_global_attrs_filename,
        variable_attrs=config['pbp_job_agent']['variable_attrs'],
    )



    print('stop')



if __name__ == "__main__":
    main()