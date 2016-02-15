#!/bin/sh

#echo "$# parameters"

if [ $# != 7 ]; then
    echo "You need to provide 7 parameters: directory, host, port, db, schema, user, password"
    exit 1
fi

ogr2ogr $1/node.shp PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT node_id, city_id, city_name, city_type, geom FROM cassini_node"
ogr2ogr $1/edge.shp PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT edge_id, start_node, end_node, road_id, road_type, length, geom FROM cassini_edge"
ogr2ogr $1/face.shp PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT face_id, geom FROM cassini_face"
ogr2ogr $1/edge_duplicate.shp PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT edge_id, start_node, end_node, geom FROM cassini_edge_duplicates"

ogr2ogr -f CSV $1/node.csv PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT node_id, city_id, city_name, city_type, ST_X(ST_Transform(geom,4326)) AS long, ST_Y(ST_Transform(geom,4326)) AS lat FROM cassini_node"
ogr2ogr -f CSV $1/edge.csv PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT edge_id, start_node, end_node, road_id, road_type, length FROM cassini_edge"

ogr2ogr -a_srs EPSG:2154 $1 PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" france_cassini_cities
ogr2ogr -a_srs EPSG:2154 $1 PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" france_cassini_roads

ogr2ogr -f CSV $1/version.csv PG:"host='$2' port='$3' user='$6' dbname='$4' password='$7' schemas=$5" -sql "SELECT last_event, tolerance, tolerance_cities FROM version"

tables=(node edge face edge_duplicate france_cassini_cities france_cassini_roads)

for item in ${tables[*]}
do
  printf "   %s\n" $item
  tar cvjf $1/$item.tgz $1/$item.shp $1/$item.dbf $1/$item.shx $1/$item.prj $1/version.csv
  rm $1/$item.shp $1/$item.dbf $1/$item.shx $1/$item.prj
done

