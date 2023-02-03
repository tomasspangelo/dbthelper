from setuptools import setup
setup(
    name='dbthelper',
    packages=['src'],
    version='0.0.1',
    install_requires=[
        "pandas-gbq==0.19.1",
        "python-dotenv==0.21.1"

    ],
    entry_points={
        'console_scripts': [
            'dbthelper=src.main:run'
        ]
    }
)
