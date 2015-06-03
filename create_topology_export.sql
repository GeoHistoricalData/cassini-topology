-- translate the input text (in french) to english for the export using the translations table
CREATE OR REPLACE FUNCTION translate(text) RETURNS text AS
  'select english from public.translations where french=$1;'
  LANGUAGE SQL IMMUTABLE;

-- create the road topology using the given tolerances
CREATE OR REPLACE FUNCTION create_road_topology(atopology varchar, tolerance double precision, tolerance_cities double precision) RETURNS VOID AS
$$
DECLARE
  topo_id integer;
  road_layer_id integer;
  face_layer_id integer;
  r record;
  city record;--export.france_cassini_cities%rowtype;
  nb integer;
  p geometry;
  n record;--france_cassini_routes_topo.node%rowtype;
  face record;--france_cassini_routes_topo.face%rowtype;
  line geometry;
  s record;
  t record;
  sql TEXT;
BEGIN
  RAISE INFO 'START';
  -- lock used tables to make sure version is right
  LOCK TABLE france_cassini, france_cassini_taches_urbaines IN ACCESS SHARE MODE NOWAIT; --ACCESS EXCLUSIVE MODE;-- NOWAIT;
  -- clean up
  RAISE INFO 'DROP TOPOLOGY';
  PERFORM topology.DropTopology(atopology);

  -- import tables and version
  RAISE INFO 'IMPORTING';
    -- create new topology schema (with lambert93)
  SELECT CreateTopology(atopology, 2154) INTO topo_id;

  -- save the version of the input data (the id of the last logged action) and the parameters used for the export (tolerances)
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.version AS SELECT max(event_id) AS last_event, $1 AS tolerance, $2 AS tolerance_cities FROM audit.logged_actions';
  EXECUTE sql USING tolerance, tolerance_cities;
  -- copy the road data
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.road AS SELECT id, translate(type) AS road_type, nom AS road_name, geom, incertain AS uncertain, commentaire AS comments, bordee_arbres AS bordered FROM public.france_cassini';
  EXECUTE sql;
  -- the roads get copied twice: once for topology manipulations and once for the actual export (no modification)
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.france_cassini_roads AS SELECT id, translate(type) AS road_type, nom AS road_name, geom, incertain AS uncertain, commentaire AS comments, bordee_arbres AS bordered FROM public.france_cassini';
  EXECUTE sql;
  -- copy the city data
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.france_cassini_cities AS SELECT id, translate(type) AS city_type, nom AS city_name , geom, commentaire AS comments, fortifiee AS fortified FROM public.france_cassini_taches_urbaines';
  EXECUTE sql;

  -- add spatial indexing to road
  sql := 'CREATE INDEX road_index ON ' || quote_ident(atopology) || '.road USING gist(geom)';
  EXECUTE sql;
  -- add spatial indexing to france_cassini_cities
  sql := 'CREATE INDEX city_index ON ' || quote_ident(atopology) || '.france_cassini_cities USING gist(geom)';
  EXECUTE sql;

  -- create road topogeometry
  SELECT AddTopoGeometryColumn(atopology, atopology, 'road', 'topo_geom', 'LINESTRING') INTO road_layer_id;
  -- create faces table, topogemetry and geometry
  --CREATE TABLE export_topology.faces(id SERIAL PRIMARY KEY);
  --SELECT AddTopoGeometryColumn(atopology, 'export_topology', 'faces', 'topo_geom', 'POLYGON') INTO face_layer_id;
  --PERFORM AddGeometryColumn ('export_topology','faces','geom',2154,'POLYGON',2, false);
  -- add spatial indexing to faces
  --CREATE INDEX faces_index ON export_topology.faces USING gist(geom);
  --CREATE UNIQUE INDEX faces_index_pk ON export_topology.faces USING btree(id);

  -- create topogeometries for roads using the given tolerance
  RAISE INFO 'CREATING TOPOGEOMETRIES FROM ROADS';
  --sql := 'UPDATE ' || quote_ident(atopology) || '.road SET topo_geom = topology.toTopoGeom(geom, ' || quote_literal(atopology) || ', $1, $2) WHERE topo_geom IS NULL';
  --EXECUTE sql USING road_layer_id, tolerance;
  sql := 'SELECT * FROM '|| quote_ident(atopology) || '.road WHERE topo_geom IS NULL';
  FOR r IN EXECUTE sql LOOP
    BEGIN
      -- create a topogeometry for each input road segment
      sql := 'UPDATE ' || quote_ident(atopology) || '.road SET topo_geom = topology.toTopoGeom(geom, ' || quote_literal(atopology) || ', $1, $2) WHERE id = $3';
      EXECUTE sql USING road_layer_id, tolerance, r.id;
      EXCEPTION
        WHEN OTHERS THEN RAISE WARNING 'Loading of record % failed: %', r.id, SQLERRM;
    END;
  END LOOP;
  
  -- add new column to count the number of corresponding roads in the original table
  RAISE INFO 'COUNTING MULTIPLE EDGES';
  sql := 'ALTER TABLE ' || quote_ident(atopology) || '.road ADD COLUMN nb_edges int';
  EXECUTE sql;
  -- count them now
  sql := 'UPDATE ' || quote_ident(atopology) || '.road SET nb_edges = (SELECT count(*) from GetTopoGeomElements(topo_geom))';
  EXECUTE sql;

  --SELECT st_remedgemodface('france_cassini_routes_topo', edge_id)
  --FROM france_cassini_routes_topo.edge_data
  --WHERE left_face = right_face;

  -- create a new sequence to give ids to the new roads (those inside the cities)
  sql := 'SELECT max(id) FROM ' || quote_ident(atopology) || '.road';
  EXECUTE sql INTO nb;
  sql := 'CREATE SEQUENCE ' || quote_ident(atopology) || '.road_id_seq' || ' START ' || (nb + 1);
  EXECUTE sql;
  sql := 'ALTER TABLE ' || quote_ident(atopology) || '.road ALTER COLUMN id SET DEFAULT nextval(' || quote_literal(atopology || '.road_id_seq') || '::regclass)';
  EXECUTE sql;

  -- create nodes and edges inside the cities
  RAISE INFO 'CREATING CITY NODES';
  sql := 'SELECT * FROM ' || quote_ident(atopology) || '.france_cassini_cities';
  FOR city IN EXECUTE sql LOOP
    BEGIN
      sql := 'SELECT count(*) FROM ' || quote_ident(atopology) || '.edge_data WHERE geom && $1 AND ST_Within(geom, $1)';
      EXECUTE sql INTO nb USING city.geom;
      IF (nb = 0) THEN
        SELECT ST_Centroid(city.geom) INTO p;
        --RAISE INFO 'No edge contained for city % with centroid %', city.id, st_astext(p);
        sql := 'SELECT * FROM ' || quote_ident(atopology) ||'.node WHERE ST_DWithin(geom, $1, $2)';
        FOR n IN EXECUTE sql USING city.geom, tolerance_cities LOOP
          SELECT ST_MakeLine(p, n.geom) INTO line;
          --RAISE INFO 'New edge with node %', n.node_id;
          EXECUTE 'INSERT INTO ' || quote_ident(atopology) || '.road(road_type, uncertain, geom, topo_geom) '
            || 'VALUES (' || quote_literal('fictive') || ', true, $1, topology.toTopoGeom($1, '|| quote_literal(atopology) || ', $2, $3))'
            USING line, road_layer_id, 0.0;
        END LOOP;
      ELSE
        --RAISE INFO '% edges contained for city %', nb, city.id;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE WARNING 'Loading of record % failed: %', city.id, SQLERRM;
    END;
  END LOOP;

  -- create new faces between roads
  --RAISE INFO 'CREATING FACES';
  --sql := 'SELECT * FROM ' || quote_ident(atopology) || '.face';
  --FOR face IN SELECT * FROM france_cassini_routes_topo.face LOOP
  --FOR face IN EXECUTE sql LOOP
  --  BEGIN
  --    SELECT count(*) INTO nb FROM export_topology.faces WHERE id = face.face_id;
  --    IF (nb = 1) THEN
  --      UPDATE export_topology.faces SET topo_geom = topology.CreateTopoGeom(atopology,3,2,ARRAY[ARRAY[face.face_id,3]]::topology.topoelementarray)
  --      WHERE id = face.face_id;
  --    ELSE
  --      INSERT INTO export_topology.faces(id,topo_geom) 
  --        VALUES (
  --          face.face_id,
  --          topology.CreateTopoGeom(atopology,3,2,ARRAY[ARRAY[face.face_id,3]]::topology.topoelementarray));
  --    END IF;
  --  EXCEPTION
  --    WHEN OTHERS THEN
  --      RAISE WARNING 'Loading of record % failed: %', face.face_id, SQLERRM;
  --  END;
  --END LOOP;

  -- give them a proper geometry
  --UPDATE export_topology.faces SET geom = topology.ST_GetFaceGeometry(atopology,id);

  --DROP TABLE IF EXISTS export.edge CASCADE;
  -- create the output edges
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.cassini_edge ('
    || 'edge_id serial NOT NULL,'
    || 'start_node integer NOT NULL,'
    || 'end_node integer NOT NULL,'
    || 'road_id integer NOT NULL,'
    || 'road_type character varying(50) NOT NULL,'
    || 'length double precision NOT NULL,'
    || 'geom geometry(LineString,2154),'
    || 'CONSTRAINT export_edge_pkey PRIMARY KEY (edge_id)'    
    || ')';
  EXECUTE sql;
  --DROP TABLE IF EXISTS export.edge_duplicates CASCADE;
  -- create the output duplicates table (to know where to check)
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.cassini_edge_duplicates ('
    || 'edge_id serial NOT NULL,'
    || 'start_node integer NOT NULL,'
    || 'end_node integer NOT NULL,'
    || 'geom geometry(LineString,2154)'
    || ')';
  EXECUTE sql;
    
  -- create a spatial index on the edges
  --CREATE INDEX export_edge_gist ON export.edge USING gist(geom);
  --DROP TABLE IF EXISTS export.node CASCADE;
  -- create the output nodes
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.cassini_node ('
    || 'node_id serial NOT NULL,'
    || 'city_id integer,'
    || 'city_name character varying(150),'
    || 'city_type character varying(150),'
    || 'geom geometry(Point,2154)'
    || ')';
  EXECUTE sql;
  -- with an index too
  --CREATE INDEX export_node_gist ON export.node USING gist(geom);

  -- DECLARE
  -- r record;
  -- s record;
  -- BEGIN
  --   FOR r IN SELECT * FROM france_cassini_routes_topo.edge_data LOOP
  --     BEGIN
  --       FOR s IN SELECT id FROM france_cassini WHERE GetTopoGeomElements(topo_geom)[1] = r.edge_id LOOP
  --         BEGIN
  --           INSERT INTO export.edge(edge_id,start_node,end_node,road_id,geom)
  --             VALUES (r.edge_id,r.start_node,r.end_node,s.id,r.geom);
  --         END;
  --       END LOOP;
  --     END;
  --   END LOOP;
  -- END;

  -- actually export the edges and the duplicates
  RAISE INFO 'EXPORTING EDGES AND DUPLICATES';
  sql := 'SELECT id, road_type, GetTopoGeomElements(topo_geom) AS topo FROM ' || quote_ident(atopology) || '.road';
  FOR r IN EXECUTE sql LOOP
    BEGIN
      sql := 'SELECT * FROM ' || quote_ident(atopology) || '.edge_data WHERE edge_id = $1';
      FOR t IN EXECUTE sql USING r.topo[1] LOOP
        BEGIN
          --INSERT INTO export.edge(edge_id,start_node,end_node,road_id,road_type,length,geom)
          --  VALUES (t.edge_id,t.start_node,t.end_node,r.id,r.type,ST_Length(t.geom),t.geom);
          sql := 'INSERT INTO ' || quote_ident(atopology) || '.cassini_edge(edge_id,start_node,end_node,road_id,road_type,length,geom) '
            || 'VALUES ($1, $2, $3, $4, $5, $6, $7)';
          EXECUTE sql USING t.edge_id,t.start_node,t.end_node,r.id,r.road_type,ST_Length(t.geom),t.geom;
          EXCEPTION
            WHEN OTHERS THEN
              --RAISE WARNING 'Duplicate % with % and % failed: %', r.id, s.topo, t.edge_id, SQLERRM;
              --INSERT INTO export.edge_duplicates(edge_id,start_node,end_node,geom)
              --  VALUES (t.edge_id,t.start_node,t.end_node,t.geom);
              sql := 'INSERT INTO ' || quote_ident(atopology) || '.cassini_edge_duplicates(edge_id,start_node,end_node,geom) '
                || 'VALUES ($1, $2, $3, $4)';
              EXECUTE sql USING t.edge_id,t.start_node,t.end_node,t.geom;
        END;
      END LOOP;
    END;
  END LOOP;

  -- export the nodes
  RAISE INFO 'EXPORTING NODES';
  sql := 'SELECT * FROM ' || quote_ident(atopology) || '.node';
  FOR n IN EXECUTE sql LOOP
    BEGIN
      --SELECT c.id, c.nom, c.type INTO s FROM export.france_cassini_cities AS c WHERE ST_Intersects(c.geom, n.geom) LIMIT 1;
      -- TODO: we should use STRICT here
      sql := 'SELECT c.id, c.city_name, c.city_type FROM ' || quote_ident(atopology) || '.france_cassini_cities AS c WHERE c.geom && $1 AND ST_Intersects(c.geom, $1) LIMIT 1';
      EXECUTE sql INTO s USING n.geom;
      --INSERT INTO export.node(node_id,city_id,city_name,city_type,geom)
      --  VALUES (n.node_id,s.id,s.nom,translate(s.type),n.geom);
      IF s IS NULL THEN
        sql := 'INSERT INTO ' || quote_ident(atopology) || '.cassini_node(node_id,city_id,city_name,city_type,geom) '
          || 'VALUES ($1,$2,$3,$4,$5)';
        EXECUTE sql USING n.node_id,NULL,NULL,NULL,n.geom;
      ELSE
        sql := 'INSERT INTO ' || quote_ident(atopology) || '.cassini_node(node_id,city_id,city_name,city_type,geom) '
          || 'VALUES ($1,$2,$3,$4,$5)';
        EXECUTE sql USING n.node_id,s.id,s.city_name,s.city_type,n.geom;
      END IF;
      EXCEPTION WHEN OTHERS THEN RAISE WARNING 'Record % failed: %', n.node_id, SQLERRM;
    END;
  END LOOP;

  RAISE INFO 'END';
  sql := 'ALTER SCHEMA ' || quote_ident(atopology) || ' OWNER TO ghdb_admin';
  EXECUTE sql;
  sql := 'GRANT USAGE ON SCHEMA ' || quote_ident(atopology) || ' TO GROUP ghdb_user';
  EXECUTE sql;
  sql := 'GRANT ALL ON ALL TABLES IN SCHEMA ' || quote_ident(atopology) || ' TO ghdb_admin';
  EXECUTE sql;
  sql := 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || quote_ident(atopology) || ' TO ghdb_user';
  EXECUTE sql;
END
$$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION create_road_topology_cassini() RETURNS VOID AS
$$
  SELECT create_road_topology('france_cassini_routes_topo', 10.0, 20.0);
$$
LANGUAGE 'sql' VOLATILE;
