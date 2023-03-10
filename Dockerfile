from thinkwhere/gdal-python
WORKDIR /app
RUN pip3 install --upgrade pip
RUN pip3 install dbfread
RUN git clone https://github.com/Cognitics/cdb-shp-geopackage-convert.git
CMD bash
