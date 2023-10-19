import json

def json_load(filename):
    with open(filename, "r", encoding="utf8") as read_file:
        result = json.load(read_file)
    return result

def json_dump(filename, data):
    with open(filename, "w",encoding="utf8") as write_file:
        json.dump(data, write_file,ensure_ascii=False)
        
def new_data_add(filename,new_data):
    data = json_load(filename)
    data += new_data
    json_dump(filename,data)        