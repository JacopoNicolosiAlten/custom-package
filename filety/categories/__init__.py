import glob, os
from importlib import import_module

file_categories_map = dict()
_relative_path = os.path.join('custom_package', 'filety', 'categories')
for category_path in glob.glob(os.path.join(_relative_path, '**.py'), recursive=True):
    basename = os.path.basename(category_path)
    if basename not in {'__init__.py', 'cateory_template.py', 'utils.py'}:
        category = basename.split('.')[0]
        module = import_module('.' + category, _relative_path.replace(os.path.sep, '.'))
        file_categories_map[category] = getattr(module, 'map')