from setuptools import setup, find_packages
import os

# Get the absolute path of the directory containing setup.py
here = os.path.abspath(os.path.dirname(__file__))
# Try to find README.md in the current directory first, then parent directory
readme_same_dir = os.path.join(here, 'README.md')
readme_parent_dir = os.path.join(os.path.dirname(here), 'README.md')

# Try to read README.md from either location
long_description = 'A package for extracting dbt column lineage - see https://github.com/canva-public/dbt-column-lineage-extractor for more details'
for readme_path in [readme_same_dir, readme_parent_dir]:
    try:
        with open(readme_path, 'r', encoding='utf-8') as f:
            long_description = f.read()
            break
    except (IOError, FileNotFoundError):
        continue

setup(
    name='dbt_column_lineage_extractor',
    version='0.1.7b2',
    description='A package for extracting dbt column lineage',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Wen Wu',
    author_email='wenwu@canva.com',
    url='https://github.com/canva-public/dbt-column-lineage-extractor',
    packages=find_packages(),
    install_requires=[
        'sqlglot[rs] == 25.24.5',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'dbt_column_lineage_direct=dbt_column_lineage_extractor.cli_direct:main',
            'dbt_column_lineage_recursive=dbt_column_lineage_extractor.cli_recursive:main',
        ],
    },
)
