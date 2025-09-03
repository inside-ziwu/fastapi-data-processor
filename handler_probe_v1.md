import json

async def main(args: Args) -> Output:
    """
    Probe to inspect the incoming args.params object.
    """
    params_type = str(type(args.params))
    
    try:
        params_content = json.dumps(args.params)
    except:
        params_content = str(args.params)

    record = {
        "fields": {
            "PROBE_PARAMS_TYPE": params_type,
            "PROBE_PARAMS_CONTENT": params_content
        }
    }

    ret: Output = {
        "records": [record]
    }
    return ret
