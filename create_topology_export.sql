-- translate the input text (in french) to english for the export using the translations table
CREATE OR REPLACE FUNCTION translate(text) RETURNS text
    AS 'select english from public.translations where french=$1;'
    LANGUAGE SQL
    IMMUTABLE;

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
  --BEGIN TOPOLOGY_EXPORT;
  -- lock used tables to make sure version is right
  LOCK TABLE france_cassini, france_cassini_taches_urbaines IN ACCESS SHARE MODE NOWAIT; --ACCESS EXCLUSIVE MODE;-- NOWAIT;
  -- clean up
  RAISE INFO 'DROP TOPOLOGY';
  PERFORM topology.DropTopology(atopology);
  --DROP SCHEMA IF EXISTS export CASCADE;
  --DROP SCHEMA IF EXISTS export_topology CASCADE;

  -- create clean schema
  --CREATE SCHEMA export AUTHORIZATION ghdb_user;
  --CREATE SCHEMA export_topology AUTHORIZATION ghdb_admin;

  -- import tables and version
  RAISE INFO 'IMPORTING';
    -- create new topology schema (with lambert93)
  SELECT CreateTopology(atopology, 2154) INTO topo_id;

  -- save the version of the input data (the id of the last logged action) and the parameters used for the export (tolerances)
  --CREATE TABLE export.version AS SELECT max(event_id) AS last_event, tolerance, tolerance_cities FROM audit.logged_actions;
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.version AS SELECT max(event_id) AS last_event, $1 AS tolerance, $2 AS tolerance_cities FROM audit.logged_actions';
  EXECUTE sql USING tolerance, tolerance_cities;
  -- copy the road data
  --CREATE TABLE export_topology.roads AS SELECT * FROM public.france_cassini;
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.road AS SELECT * FROM public.france_cassini';
  EXECUTE sql;
  -- the roads get copied twice: once for topology manipulations and once for the actual export (no modification)
  --CREATE TABLE export.france_cassini_roads AS SELECT id, translate(type) AS road_type, nom AS road_name , geom, incertain AS uncertain, commentaire AS comments, bordee_arbres AS bordered FROM public.france_cassini;
  sql := 'CREATE TABLE ' || quote_ident(atopology) || '.france_cassini_roads AS SELECT id, translate(type) AS road_type, nom AS road_name, geom, incertain AS uncertain, commentaire AS comments, bordee_arbres AS bordered FROM public.france_cassini';
  EXECUTE sql;
  -- copy the city data
  --CREATE TABLE export.france_cassini_cities AS SELECT id, translate(type) AS city_type, nom AS city_name , geom, commentaire AS comments, fortifiee AS fortified FROM public.france_cassini_taches_urbaines;
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
  --FOR r IN SELECT * FROM export_topology.roads WHERE topo_geom IS NULL LOOP
  --FOR r IN EXECUTE sql LOOP
  --  BEGIN
  --    -- create a topogeometry for each input road segment
  --    UPDATE export_topology.roads SET topo_geom = topology.toTopoGeom(geom, atopology, 1, tolerance) WHERE id = r.id;
  --  EXCEPTION
  --    WHEN OTHERS THEN RAISE WARNING 'Loading of record % failed: %', r.id, SQLERRM;
  --  END;
  --END LOOP;

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
  --ALTER TABLE export_topology.roads ADD COLUMN nb_edges int;
  sql := 'ALTER TABLE ' || quote_ident(atopology) || '.road ADD COLUMN nb_edges int';
  EXECUTE sql;
  -- count them now
  --UPDATE export_topology.roads AS roads SET nb_edges = (SELECT count(*) from (select GetTopoGeomElements(topo_geom) from export_topology.roads as f where f.id = roads.id) AS tmp);
  sql := 'UPDATE ' || quote_ident(atopology) || '.road SET nb_edges = (SELECT count(*) from GetTopoGeomElements(topo_geom))';
  EXECUTE sql;

  --SELECT st_remedgemodface('france_cassini_routes_topo', edge_id)
  --FROM france_cassini_routes_topo.edge_data
  --WHERE left_face = right_face;

  -- create a new sequence to give ids to the new roads (those inside the cities)
  --CREATE SEQUENCE export_topology.roads_id_seq;
  --PERFORM setval('export_topology.roads_id_seq', max(id)) FROM export_topology.roads;
  sql := 'SELECT max(id) FROM ' || quote_ident(atopology) || '.road';
  EXECUTE sql INTO nb;
  sql := 'CREATE SEQUENCE ' || quote_ident(atopology) || '.road_id_seq' || ' START ' || (nb + 1);
  EXECUTE sql;
  --ALTER TABLE export_topology.roads ALTER COLUMN id SET DEFAULT nextval('export_topology.roads_id_seq'::regclass);
  sql := 'ALTER TABLE ' || quote_ident(atopology) || '.road ALTER COLUMN id SET DEFAULT nextval(' || quote_literal(atopology || '.road_id_seq') || '::regclass)';
  EXECUTE sql;

  -- create nodes and edges inside the cities
  RAISE INFO 'CREATING CITY NODES';
  --FOR city IN SELECT * FROM export.france_cassini_cities LOOP
  sql := 'SELECT * FROM ' || quote_ident(atopology) || '.france_cassini_cities';
  FOR city IN EXECUTE sql LOOP
    BEGIN
      --SELECT count(*) INTO nb FROM france_cassini_routes_topo.edge_data
      --  WHERE ST_Within(geom, city.geom);
      sql := 'SELECT count(*) FROM ' || quote_ident(atopology) || '.edge_data WHERE geom && $1 AND ST_Within(geom, $1)';
      EXECUTE sql INTO nb USING city.geom;
      IF (nb = 0) THEN
        SELECT ST_Centroid(city.geom) INTO p;
        --RAISE INFO 'No edge contained for city % with centroid %', city.id, st_astext(p);
        --FOR n IN SELECT * FROM france_cassini_routes_topo.node
        --  WHERE ST_Intersects(geom, ST_Buffer(city.geom,tolerance_cities))
        sql := 'SELECT * FROM ' || quote_ident(atopology) ||'.node WHERE ST_DWithin(geom, $1, $2)';
        FOR n IN EXECUTE sql USING city.geom, tolerance_cities LOOP
          SELECT ST_MakeLine(p, n.geom) INTO line;
          --RAISE INFO 'New edge with node %', n.node_id;
          --INSERT INTO export_topology.roads(type, incertain, geom, topo_geom) VALUES ('fictive',true,line,topology.toTopoGeom(line, atopology, 1, 0.0));
          EXECUTE 'INSERT INTO ' || quote_ident(atopology) || '.road(type, incertain, geom, topo_geom) '
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
  /*
  CREATE TABLE export.edge
  (
    edge_id serial NOT NULL,
    start_node integer NOT NULL,
    end_node integer NOT NULL,
    road_id integer NOT NULL,
    road_type character varying(50) NOT NULL,
    length double precision NOT NULL,
    geom geometry(LineString,2154),
    CONSTRAINT export_edge_pkey PRIMARY KEY (edge_id)
  );
  */
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
  /*
  CREATE TABLE export.edge_duplicates
  (
    edge_id serial NOT NULL,
    start_node integer NOT NULL,
    end_node integer NOT NULL,
    geom geometry(LineString,2154)
  );
  */
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
  /*
  CREATE TABLE export.node
  (
    node_id serial NOT NULL,
    city_id integer,
    city_name character varying(150),
    city_type character varying(150),
    geom geometry(Point,2154),
    CONSTRAINT node_primary_key PRIMARY KEY (node_id)
  );
  */
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
  /*
  FOR r IN SELECT * FROM export_topology.roads LOOP
    BEGIN
      FOR s IN SELECT GetTopoGeomElements(topo_geom) AS topo FROM export_topology.roads WHERE id = r.id LOOP
        BEGIN
          sql := 'SELECT * FROM ' || quote_ident(atopology) || '.edge_data WHERE edge_id = $1';
          FOR t IN EXECUTE sql USING s.topo[1] LOOP
            BEGIN
              INSERT INTO export.edge(edge_id,start_node,end_node,road_id,road_type,length,geom)
                VALUES (t.edge_id,t.start_node,t.end_node,r.id,translate(r.type),ST_Length(t.geom),t.geom);
              EXCEPTION
                WHEN OTHERS THEN
                  --RAISE WARNING 'Duplicate % with % and % failed: %', r.id, s.topo, t.edge_id, SQLERRM;
                  INSERT INTO export.edge_duplicates(edge_id,start_node,end_node,geom)
                    VALUES (t.edge_id,t.start_node,t.end_node,t.geom);
            END;
          END LOOP;
        END;
      END LOOP;
    END;
  END LOOP;
  */
  sql := 'SELECT id, translate(type) AS type, GetTopoGeomElements(topo_geom) AS topo FROM ' || quote_ident(atopology) || '.road';
  FOR r IN EXECUTE sql LOOP
    BEGIN
      sql := 'SELECT * FROM ' || quote_ident(atopology) || '.edge_data WHERE edge_id = $1';
      FOR t IN EXECUTE sql USING r.topo[1] LOOP
        BEGIN
          --INSERT INTO export.edge(edge_id,start_node,end_node,road_id,road_type,length,geom)
          --  VALUES (t.edge_id,t.start_node,t.end_node,r.id,r.type,ST_Length(t.geom),t.geom);
          sql := 'INSERT INTO ' || quote_ident(atopology) || '.cassini_edge(edge_id,start_node,end_node,road_id,road_type,length,geom) '
            || 'VALUES ($1, $2, $3, $4, $5, $6, $7)';
          EXECUTE sql USING t.edge_id,t.start_node,t.end_node,r.id,r.type,ST_Length(t.geom),t.geom;
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

  -- DECLARE
  -- r record;
  -- BEGIN
  --   FOR r IN SELECT n.node_id, n.geom, c.id, c.nom FROM france_cassini_routes_topo.node AS n JOIN france_cassini_taches_urbaines AS c ON ST_Intersects(c.geom, n.geom)
  --   LOOP
  --     BEGIN
  --       INSERT INTO export.node(node_id,city_id,city_name,geom)
  --         VALUES (r.node_id,r.id,r.nom,r.geom);
  --       EXCEPTION WHEN OTHERS THEN
  --         RAISE WARNING 'Record % with % failed: %', r.node_id, r.id, SQLERRM;
  --     END;
  --   END LOOP;
  -- END;

  -- export the nodes
  RAISE INFO 'EXPORTING NODES';
  sql := 'SELECT * FROM ' || quote_ident(atopology) || '.node';
  FOR n IN EXECUTE sql LOOP
    BEGIN
      --SELECT c.id, c.nom, c.type INTO s FROM export.france_cassini_cities AS c WHERE ST_Intersects(c.geom, n.geom) LIMIT 1;
      -- TODO: we should use STRICT here
      sql := 'SELECT c.id, c.nom, c.type FROM ' || quote_ident(atopology) || '.france_cassini_cities AS c WHERE c.geom && $1 AND ST_Intersects(c.geom, $1) LIMIT 1';
      EXECUTE sql INTO s USING n.geom;
      --INSERT INTO export.node(node_id,city_id,city_name,city_type,geom)
      --  VALUES (n.node_id,s.id,s.nom,translate(s.type),n.geom);
      sql := 'INSERT INTO ' || quote_ident(atopology) || '.node(node_id,city_id,city_name,city_type,geom) '
        || 'VALUES ($1,$2,$3,$4,$5)';
      EXECUTE sql USING n.node_id,s.id,s.nom,translate(s.type),n.geom;
      EXCEPTION WHEN OTHERS THEN RAISE WARNING 'Record % with % failed: %', n.node_id, s.id, SQLERRM;
    END;
  END LOOP;

  --select * from france_cassini_routes_topo.relation where element_id = 20;
  --select t.el[1] FROM (select GetTopoGeomElements(topo_geom)AS el from france_cassini WHERE id=25267) AS t;
  --select GetTopoGeomElements(topo_geom) from france_cassini WHERE id=25267;

  RAISE INFO 'END';

  --GRANT SELECT ON ALL TABLES IN SCHEMA export TO ghdb_user;

  -- clean up?
  --GRANT ALL ON ALL TABLES IN SCHEMA export_topology TO ghdb_admin;
  --  GRANT ALL ON ALL TABLES IN SCHEMA france_cassini_routes_topo TO ghdb_admin;
  --sql := 'GRANT ALL ON ALL TABLES IN SCHEMA ' || quote_ident(atopology) || ' TO ghdb_admin';
  --EXECUTE sql;
  -- COMMIT;
  -- COMMIT TOPOLOGY_EXPORT;
END
$$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION create_road_topology_cassini() RETURNS VOID AS
$$
  SELECT create_road_topology('france_cassini_routes_topo', 10.0, 20.0);
$$
LANGUAGE 'sql' VOLATILE;
