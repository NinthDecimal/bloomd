from setuptools import setup, Command
import bloomd

# Get the long description by reading the README
try:
    readme_content = open("README.rst").read()
except:
    readme_content = ""

# Create the actual setup method
setup(name='bloomd',
      version=bloomd.__version__,
      description='Lightweight server to manage probabilistic sets based on bloom filters',
      long_description=readme_content,
      author='Armon Dadgar',
      author_email='armon@kiip.me',
      maintainer='Armon Dadgar',
      maintainer_email='armon@kiip.me',
      url="https://github.com/kiip/bloomd/",
      license="MIT License",
      keywords=["bloom", "filter","server","twisted"],
      packages=['bloomd','bloomd.bin'],
      entry_points = {
        "console_scripts": ["bloomd = bloomd.bin.bloomd:main"],
      },
      install_requires = ["twisted==11.0.0","pyblooming==0.3.0"],
      classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet",
    ]
      )
