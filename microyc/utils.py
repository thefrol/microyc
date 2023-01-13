from pathlib import Path
import uuid

def combine_path(dir, file):
    filename: Path = Path(file)
    directory: Path = Path(dir)
    return str(directory / filename)

def get_filename(path):
    return Path(path).name

def get_extension(path):
    return Path().suffix

def generate_unique():
    return str(uuid.uuid4())

def generate_new_filename(path):
    file=Path(path)
    extension=file.suffix
    new_name=generate_unique()
    return f'{new_name}{extension}'
