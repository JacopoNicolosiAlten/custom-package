import glob, os
from importlib import import_module

file_categories_map = dict()
_relative_path = os.path.join('custom_package', 'filety', 'categories')
for category_path in glob.glob(os.path.join(_relative_path, '**.py'), recursive=True):
    if '__init__.py' not in category_path:
        category = os.path.basename(category_path).split('.')[0]
        module = import_module('.' + category, _relative_path.replace(os.path.sep, '.'))
        file_categories_map[category] = getattr(module, 'map')