import psycopg2
import traceback
from psycopg2 import sql

# Conexión a la base de datos PostgreSQL con los parámetros proporcionados
try:
    connection = psycopg2.connect(
        host="openproject_sucs_postgres",         # Host de la base de datos
        port="5432",                      # Puerto de la base de datos
        dbname="openproject",           # Nombre de la base de datos
        user="postgres",                # Usuario de la base de datos
        password="p4ssw0rd"             # Contraseña de la base de datos
    )

    cursor = connection.cursor()

    # Definir la consulta SQL
    query = """
    INSERT INTO public.work_packages(
        type_id,
        project_id,
        subject,
        description,
        status_id,
        priority_id,
        author_id,
        lock_version,
        created_at,
        updated_at,
        schedule_manually,
        ignore_non_working_days
    )
    SELECT
        8,
        44,
        s.num_suc,
        '',
        18,
        8,
        4,
        2,
        NOW(),
        NOW(),
        false,
        false
    FROM dblink(
        'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
        'SELECT CAST(num_suc AS TEXT) FROM escapex."SucMarco"'
    ) AS s(num_suc TEXT)
    WHERE NOT EXISTS (
        SELECT 1 FROM public.work_packages w WHERE w.subject = s.num_suc
    )
    AND s.num_suc NOT LIKE '332%';
    """

    query2 = """
WITH datos AS (
    WITH escapex_data AS (
        SELECT num_suc, cleaned_historial_estados_json
        FROM dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            'SELECT num_suc, cleaned_historial_estados_json FROM escapex.sucsestados_view WHERE num_suc LIKE ''907%'' ORDER BY num_suc ASC'
        ) AS se(num_suc TEXT, cleaned_historial_estados_json JSONB)
    )
    SELECT
        wp.id AS work_package_id,
        (jsonb_array_elements(se.cleaned_historial_estados_json::jsonb)->>'estado') AS estado,
        (jsonb_array_elements(se.cleaned_historial_estados_json::jsonb)->>'fecha')::timestamp AS fecha
    FROM
        public.work_packages wp
    JOIN escapex_data se ON wp.subject = se.num_suc
)
INSERT INTO public.custom_values (customized_type, customized_id, custom_field_id, value)
SELECT
    'WorkPackage',
    datos.work_package_id,
    fields.id,
    NULL --TO_CHAR(datos.fecha, 'YY-MM-DD')
FROM datos
JOIN public.custom_fields AS fields ON fields.name = datos.estado
WHERE NOT EXISTS (
    SELECT 1
    FROM public.custom_values cv
    WHERE cv.customized_type = 'WorkPackage'
      AND cv.customized_id = datos.work_package_id
      AND cv.custom_field_id = fields.id
);
    """

    query3 = """
        WITH escapex_data AS (
        SELECT num_suc, cleaned_historial_estados_json
        FROM dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            'SELECT num_suc, cleaned_historial_estados_json FROM escapex.sucsestados_view WHERE num_suc LIKE ''907%'' ORDER BY num_suc ASC'
        ) AS se(num_suc TEXT, cleaned_historial_estados_json JSONB)
        ),
                exploded_data AS (
    SELECT
        wp.id AS work_package_id,
        estado_fecha.estado,
        TO_CHAR(estado_fecha.fecha, 'YYYY-MM-DD') AS fecha,
        ROW_NUMBER() OVER (
            PARTITION BY wp.id, estado_fecha.estado
            ORDER BY estado_fecha.fecha DESC
        ) AS rn
    FROM
        public.work_packages wp
    JOIN escapex_data se ON wp.subject = se.num_suc,
        LATERAL (
            SELECT
                (value->>'estado') AS estado,
                (value->>'fecha')::timestamp AS fecha
            FROM jsonb_array_elements(se.cleaned_historial_estados_json::jsonb) AS value
        ) AS estado_fecha
),
datos_filtrados AS (
    SELECT work_package_id, estado, fecha
    FROM exploded_data
    WHERE rn = 1  -- Solo nos quedamos con la entrada más reciente por estado
),
actualizar AS (
    SELECT
        cv.id AS custom_value_id,
        df.fecha
    FROM custom_values cv
    INNER JOIN datos_filtrados df
        ON cv.customized_id = df.work_package_id
    INNER JOIN custom_fields cf
        ON cf.id = cv.custom_field_id AND cf.name = df.estado
)
UPDATE custom_values cv
SET value = actualizar.fecha
FROM actualizar
WHERE cv.id = actualizar.custom_value_id;
    """

    query4 = """
    WITH datos AS (
        SELECT wp.id AS work_package_id, sm.estado, sm.fecha
        FROM
        dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            '
                WITH datos AS (
                    SELECT
                        CAST(sm.num_suc AS TEXT) AS num_suc,
                        CAST(sm.conductos_instalados AS TEXT) AS conductos_instalados,
                        CAST(sm.conductos_utilizados AS TEXT) AS conductos_utilizados,
                        CAST(COUNT(smc.longitud_cable_dm) AS TEXT) AS longitud_cable_dm,
                        CASE
                            WHEN COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%POSTE%'') > 0 THEN ''33''
                            ELSE ''34''
                        END AS tiene_poste
                    FROM escapex."SucMarco" sm
                    INNER JOIN escapex."SucMarcoElement" sme
                        ON sm.id = sme.suc_marco_id
                    INNER JOIN escapex."SucMarcoConduct" smc
                        ON sm.id = smc.suc_marco_id
                    WHERE sm.id_operador = 1
                    GROUP BY sm.num_suc, sm.conductos_instalados, sm.conductos_utilizados
                )
                SELECT num_suc, ''SUB INSTALADO (m)'' AS estado, conductos_instalados AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''SUB UTILIZADO (m)'' AS estado, conductos_utilizados AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''MALLA GEO. (m)'' AS estado, longitud_cable_dm AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''ES DE POSTE (SI/NO)'' AS estado, tiene_poste AS fecha FROM datos;
            '
        ) AS sm(num_suc TEXT, estado TEXT, fecha TEXT)
        INNER JOIN public."work_packages" wp
        ON wp.subject = sm.num_suc
        ORDER BY wp.id, sm.estado
    )
    INSERT INTO public.custom_values (customized_type, customized_id, custom_field_id, value)
    SELECT
        'WorkPackage',
        datos.work_package_id,
        fields.id,
        NULL --TO_CHAR(datos.fecha, 'YY-MM-DD')
    FROM datos
    JOIN public.custom_fields AS fields ON fields.name = datos.estado
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.custom_values cv
        WHERE cv.customized_type = 'WorkPackage'
          AND cv.customized_id = datos.work_package_id
          AND cv.custom_field_id = fields.id
    );
    """

    query5 = """
    WITH datos AS (
        SELECT wp.id AS work_package_id, sm.estado, sm.fecha
        FROM
        dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            '
                WITH datos AS (
                    SELECT
                        CAST(sm.num_suc AS TEXT) AS num_suc,
                        CAST(sm.conductos_instalados AS TEXT) AS conductos_instalados,
                        CAST(sm.conductos_utilizados AS TEXT) AS conductos_utilizados,
                        CAST(COUNT(smc.longitud_cable_dm) AS TEXT) AS longitud_cable_dm,
                        CASE
                            WHEN COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%POSTE%'') > 0 THEN ''33''
                            ELSE ''34''
                        END AS tiene_poste
                    FROM escapex."SucMarco" sm
                    INNER JOIN escapex."SucMarcoElement" sme
                        ON sm.id = sme.suc_marco_id
                    INNER JOIN escapex."SucMarcoConduct" smc
                        ON sm.id = smc.suc_marco_id
                    WHERE sm.id_operador = 1
                    GROUP BY sm.num_suc, sm.conductos_instalados, sm.conductos_utilizados
                )
                SELECT num_suc, ''SUB INSTALADO (m)'' AS estado, conductos_instalados AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''SUB UTILIZADO (m)'' AS estado, conductos_utilizados AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''MALLA GEO. (m)'' AS estado, longitud_cable_dm AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''ES DE POSTE (SI/NO)'' AS estado, tiene_poste AS fecha FROM datos;
            '
        ) AS sm(num_suc TEXT, estado TEXT, fecha TEXT)
        INNER JOIN public."work_packages" wp
        ON wp.subject = sm.num_suc
        ORDER BY wp.id, sm.estado
    ),
    actualizar AS (
        SELECT
            cv.id AS custom_value_id,
            d.fecha
        FROM custom_values cv
        INNER JOIN datos d
            ON cv.customized_id = d.work_package_id
        INNER JOIN custom_fields cf
            ON cf.id = cv.custom_field_id AND cf.name = d.estado
    )
    UPDATE custom_values cv
    SET value = actualizar.fecha
    FROM actualizar
    WHERE cv.id = actualizar.custom_value_id;
    """


    query6 = """
    WITH datos AS (
        SELECT wp.id AS work_package_id, sm.estado, sm.fecha
        FROM
        dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            '
                SELECT CAST(num_suc AS TEXT) AS num_suc, ''ALTA SOLICITUD'' AS estado, CAST(sm.fecha_solicitud AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''COSTE SUSTITUCION'' AS estado, CAST(sm.coste_postes AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                        SELECT CAST(num_suc AS TEXT) AS num_suc, ''REPLANTEO CONJUNTO'' AS estado, CAST(sm.fecha_replanteo AS TEXT) AS fecha
            FROM escapex."SucMarco" sm
            WHERE id_operador = 1


                UNION ALL

                SELECT
    CAST(sm.num_suc AS TEXT) AS num_suc,
    ''MUNICIPIO'' AS estado,
    mun.nameunit AS fecha
FROM escapex."SucMarco" sm
JOIN escapex."SucMarcoElement" se ON sm.id = se.suc_marco_id
JOIN escapex."Manholes" man ON se.gid = man.gid_element
JOIN spain."Municipios" mun ON ST_Intersects(man.geom, mun.geom)
WHERE sm.id_operador = 1

                UNION ALL

SELECT
    CAST(sm.num_suc AS TEXT) AS num_suc,
    ''CENTRAL'' AS estado,
    ce.te_nombre_central AS fecha
FROM escapex."SucMarco" sm
JOIN escapex."SucMarcoElement" se ON sm.id = se.suc_marco_id
JOIN escapex."Manholes" man ON se.gid = man.gid_element
JOIN escapex."CentralArea" ce ON ST_Intersects(man.geom, ce.geom)
WHERE sm.id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''SUSTITUCION POSTES'' AS estado, CAST(sm.num_postes AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''ES INVIABLE OCULTA'' AS estado,
                CASE WHEN sm.no_ejecucion_proyecto = TRUE THEN ''SI''
                    WHEN sm.no_ejecucion_proyecto = FALSE THEN ''NO''
                    ELSE ''DESCONOCIDO''
                END AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''REGISTROS FINALES TRAS AR Y MD FACILITADAS'' AS estado,
                CAST(COUNT(smd.id) AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                LEFT JOIN escapex."SucMarcoMd" smd ON smd.suc_marco_id = sm.id
                WHERE sm.id_operador = 1
                GROUP BY num_suc

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''CODIGO MIGA'' AS estado, CAST(sm.miga AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''INICIO EJECUCION'' AS estado, CAST(sm.trabajos_provision_fecha_inicio AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''LIMITE EJECUCION'' AS estado, CAST(sm.trabajos_provision_fecha_fin AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''ESTADO ACTUAL NEON'' AS estado, CAST(sm.estado AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''PROVINCIA'' AS estado, CAST(sm.provincia AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''F. INICIO OBRA'' AS estado, CAST(sm.fecha_inicio_obras AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''F. FINAL OBRA'' AS estado, CAST(sm.fecha_fin_obras AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''F. SOLIC. PERMISOS'' AS estado, CAST(sm.fecha_solicitud_permisos AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''HORA REPLANTEO'' AS estado, CAST(sm.hora_replanteo AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''OBSERVACIONES NEON'' AS estado, CAST(sm.observaciones_proyecto_postes AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''Nª CR'' AS estado,
                    CAST(COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%CR%'' OR (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%'')) AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                INNER JOIN escapex."SucMarcoElement" sme
                    ON sm.id = sme.suc_marco_id
                WHERE id_operador = 1
                GROUP BY num_suc

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''Nª ARQ'' AS estado,
                    CAST(COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%Arq%'' OR (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones NOT LIKE ''%CR%'')) AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                INNER JOIN escapex."SucMarcoElement" sme
                    ON sm.id = sme.suc_marco_id
                WHERE id_operador = 1
                GROUP BY num_suc

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''Nª POSTES'' AS estado,
                    CAST(COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%POSTE%'') AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                INNER JOIN escapex."SucMarcoElement" sme
                    ON sm.id = sme.suc_marco_id
                WHERE id_operador = 1
                GROUP BY num_suc
            '
        ) AS sm(num_suc TEXT, estado TEXT, fecha TEXT)
        INNER JOIN public."work_packages" wp
        ON wp.subject = sm.num_suc
        ORDER BY wp.id, sm.estado
    )
    INSERT INTO public.custom_values (customized_type, customized_id, custom_field_id, value)
    SELECT
        'WorkPackage',
        datos.work_package_id,
        fields.id,
        NULL --TO_CHAR(datos.fecha, 'YY-MM-DD')
    FROM datos
    JOIN public.custom_fields AS fields ON fields.name = datos.estado
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.custom_values cv
        WHERE cv.customized_type = 'WorkPackage'
          AND cv.customized_id = datos.work_package_id
          AND cv.custom_field_id = fields.id
    );
    """


    query7 = """
    WITH datos AS (
        SELECT wp.id as work_package_id, sm.estado, sm.fecha
        FROM
        dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            '
                SELECT CAST(num_suc AS TEXT) AS num_suc, ''ALTA SOLICITUD'' AS estado, CAST(sm.fecha_solicitud AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''COSTE SUSTITUCION'' AS estado, CAST(sm.coste_postes AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                        SELECT CAST(num_suc AS TEXT) AS num_suc, ''REPLANTEO CONJUNTO'' AS estado, CAST(sm.fecha_replanteo AS TEXT) AS fecha
            FROM escapex."SucMarco" sm
            WHERE id_operador = 1

                UNION ALL

SELECT
    CAST(sm.num_suc AS TEXT) AS num_suc,
    ''MUNICIPIO'' AS estado,
    mun.nameunit AS fecha
FROM escapex."SucMarco" sm
JOIN escapex."SucMarcoElement" se ON sm.id = se.suc_marco_id
JOIN escapex."Manholes" man ON se.gid = man.gid_element
JOIN spain."Municipios" mun ON ST_Intersects(man.geom, mun.geom)
WHERE sm.id_operador = 1


                UNION ALL

SELECT
    CAST(sm.num_suc AS TEXT) AS num_suc,
    ''CENTRAL'' AS estado,
    ce.te_nombre_central AS fecha
FROM escapex."SucMarco" sm
JOIN escapex."SucMarcoElement" se ON sm.id = se.suc_marco_id
JOIN escapex."Manholes" man ON se.gid = man.gid_element
JOIN escapex."CentralArea" ce ON ST_Intersects(man.geom, ce.geom)
WHERE sm.id_operador = 1



                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''SUSTITUCION POSTES'' AS estado, CAST(sm.num_postes AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''ES INVIABLE OCULTA'' AS estado,
                CASE WHEN sm.no_ejecucion_proyecto = TRUE THEN ''SI''
                    WHEN sm.no_ejecucion_proyecto = FALSE THEN ''NO''
                    ELSE ''DESCONOCIDO''
                END AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''REGISTROS FINALES TRAS AR Y MD FACILITADAS'' AS estado,
                CAST(COUNT(smd.id) AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                LEFT JOIN escapex."SucMarcoMd" smd ON smd.suc_marco_id = sm.id
                WHERE sm.id_operador = 1
                GROUP BY num_suc

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''CODIGO MIGA'' AS estado, CAST(sm.miga AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''ESTADO ACTUAL NEON'' AS estado, CAST(sm.estado AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''PROVINCIA'' AS estado, CAST(sm.provincia AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''INICIO EJECUCION'' AS estado, CAST(sm.trabajos_provision_fecha_inicio AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''LIMITE EJECUCION'' AS estado, CAST(sm.trabajos_provision_fecha_fin AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''F. INICIO OBRA'' AS estado, CAST(sm.fecha_inicio_obras AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''F. FINAL OBRA'' AS estado, CAST(sm.fecha_fin_obras AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''F. SOLIC. PERMISOS'' AS estado, CAST(sm.fecha_solicitud_permisos AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''HORA REPLANTEO'' AS estado, CAST(sm.hora_replanteo AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''OBSERVACIONES NEON'' AS estado, CAST(sm.observaciones_proyecto_postes AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                WHERE id_operador = 1

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''Nª CR'' AS estado,
                    CAST(COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%CR%'' OR (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%'')) AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                INNER JOIN escapex."SucMarcoElement" sme
                    ON sm.id = sme.suc_marco_id
                WHERE id_operador = 1
                GROUP BY num_suc

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''Nª ARQ'' AS estado,
                    CAST(COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%Arq%'' OR (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones NOT LIKE ''%CR%'')) AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                INNER JOIN escapex."SucMarcoElement" sme
                    ON sm.id = sme.suc_marco_id
                WHERE id_operador = 1
                GROUP BY num_suc

                UNION ALL

                SELECT CAST(num_suc AS TEXT) AS num_suc, ''Nª POSTES'' AS estado,
                    CAST(COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%POSTE%'') AS TEXT) AS fecha
                FROM escapex."SucMarco" sm
                INNER JOIN escapex."SucMarcoElement" sme
                    ON sm.id = sme.suc_marco_id
                WHERE id_operador = 1
                GROUP BY num_suc
            '
        ) AS sm(num_suc TEXT, estado TEXT, fecha TEXT)
        INNER JOIN public."work_packages" wp
        ON wp.subject = sm.num_suc
        ORDER BY wp.id, sm.estado
    ),
    actualizar AS (
        SELECT
            cv.id AS custom_value_id,
            d.fecha
        FROM custom_values cv
        INNER JOIN datos d
            ON cv.customized_id = d.work_package_id
        INNER JOIN custom_fields cf
            ON cf.id = cv.custom_field_id AND cf.name = d.estado
    )
    UPDATE custom_values cv
    SET value = actualizar.fecha
    FROM actualizar
    WHERE cv.id = actualizar.custom_value_id;
    """
    query8 = """
    UPDATE custom_values cfv
    SET value = REPLACE(value, ',', '.')
    FROM custom_fields cf
    WHERE cfv.custom_field_id = cf.id
    AND cf.field_format = 'float'
    AND cfv.value LIKE '%,%';
    """

    query9 = """
    UPDATE public.custom_values
    SET value = REPLACE(REPLACE(value, 'SI', '33'), 'NO', '34')
    WHERE value IN ('SI', 'NO')
    AND custom_field_id = 131;
    """
    query10 = """
WITH datos AS (
        SELECT wp.id AS work_package_id, sm.estado, sm.fecha
        FROM
        dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            '
                WITH datos AS (
                    SELECT
  num_suc,
  sm.miga,
  estado,

  COUNT(*) FILTER (
    WHERE
      (sme.tipo_registro ILIKE ''%CR%'' OR
       (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%''))
      AND uso <> ''0''
  ) AS num_CR,

  COUNT(*) FILTER (
    WHERE
      (sme.tipo_registro ILIKE ''%CR%'' OR
       (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%''))
      AND uso = ''0''
  ) AS num_CR0,

  COUNT(*) FILTER (
    WHERE
      sme.tipo_registro ILIKE ''%Arq%'' OR
      (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones NOT LIKE ''%CR%'')
  ) AS num_arq,

  COUNT(*) FILTER (
    WHERE sme.tipo_registro ILIKE ''%ARM%''
  ) AS num_ARM,

  COUNT(*) FILTER (
    WHERE sme.tipo_registro ILIKE ''%POSTE%''
  ) AS num_Poste,

  COUNT(*) FILTER (
    WHERE sme.tipo_registro ILIKE ''%CANAL%'' OR sme.tipo_registro IS NULL
  ) AS canalizado,

  COUNT(*) FILTER (
    WHERE NOT (
      (sme.tipo_registro ILIKE ''%CR%'' OR
       (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%''))
      OR (sme.tipo_registro ILIKE ''%Arq%'' OR
          (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones NOT LIKE ''%CR%''))
      OR sme.tipo_registro ILIKE ''%ARM%''
      OR sme.tipo_registro ILIKE ''%POSTE%''
      OR sme.tipo_registro ILIKE ''%CANAL%''
      OR sme.tipo_registro IS NULL
    )
  ) AS Pedestal,

  COUNT(*) AS total,
  json_agg(tipo_registro) AS tipo_registro,
  json_agg(uso) AS uso

FROM escapex."SucMarco" sm
INNER JOIN escapex."SucMarcoElement" sme
  ON sm.id = sme.suc_marco_id
WHERE id_operador = 1
GROUP BY num_suc, sm.miga, estado
                )
                SELECT num_suc, ''Nª CR "USO-0"'' AS estado, num_cr0 AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''Nª AISLADA'' AS estado, canalizado AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''Nª PEDESTAL'' AS estado, pedestal AS fecha FROM datos
            '
        ) AS sm(num_suc TEXT, estado TEXT, fecha TEXT)
        INNER JOIN public."work_packages" wp
        ON wp.subject = sm.num_suc
        ORDER BY wp.id, sm.estado
    )
    INSERT INTO public.custom_values (customized_type, customized_id, custom_field_id, value)
    SELECT
        'WorkPackage',
        datos.work_package_id,
        fields.id,
        NULL --TO_CHAR(datos.fecha, 'YY-MM-DD')
    FROM datos
    JOIN public.custom_fields AS fields ON fields.name = datos.estado
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.custom_values cv
        WHERE cv.customized_type = 'WorkPackage'
          AND cv.customized_id = datos.work_package_id
          AND cv.custom_field_id = fields.id
    );
    """
    query11 = """
WITH datos AS (
        SELECT wp.id AS work_package_id, sm.estado, sm.fecha
        FROM
        dblink(
            'host=suc.insdosl.com port=5001 dbname=PRO user=insdo_backup password=1nSd0%24',
            '
                WITH datos AS (
                    SELECT
  num_suc,
  sm.miga,
  estado,

  COUNT(*) FILTER (
    WHERE
      (sme.tipo_registro ILIKE ''%CR%'' OR
       (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%''))
      AND uso <> ''0''
  ) AS num_CR,

  COUNT(*) FILTER (
    WHERE
      (sme.tipo_registro ILIKE ''%CR%'' OR
       (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%''))
      AND uso = ''0''
  ) AS num_CR0,

  COUNT(*) FILTER (
    WHERE
      sme.tipo_registro ILIKE ''%Arq%'' OR
      (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones NOT LIKE ''%CR%'')
  ) AS num_arq,

  COUNT(*) FILTER (
    WHERE sme.tipo_registro ILIKE ''%ARM%''
  ) AS num_ARM,

  COUNT(*) FILTER (
    WHERE sme.tipo_registro ILIKE ''%POSTE%''
  ) AS num_Poste,

  COUNT(*) FILTER (
    WHERE sme.tipo_registro ILIKE ''%CANAL%'' OR sme.tipo_registro IS NULL
  ) AS canalizado,

  COUNT(*) FILTER (
    WHERE NOT (
      (sme.tipo_registro ILIKE ''%CR%'' OR
       (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones LIKE ''%CR%''))
      OR (sme.tipo_registro ILIKE ''%Arq%'' OR
          (sme.tipo_registro ILIKE ''OTROS'' AND sme.observaciones NOT LIKE ''%CR%''))
      OR sme.tipo_registro ILIKE ''%ARM%''
      OR sme.tipo_registro ILIKE ''%POSTE%''
      OR sme.tipo_registro ILIKE ''%CANAL%''
      OR sme.tipo_registro IS NULL
    )
  ) AS Pedestal,

  COUNT(*) AS total,
  json_agg(tipo_registro) AS tipo_registro,
  json_agg(uso) AS uso

FROM escapex."SucMarco" sm
INNER JOIN escapex."SucMarcoElement" sme
  ON sm.id = sme.suc_marco_id
WHERE id_operador = 1
GROUP BY num_suc, sm.miga, estado
                )
                SELECT num_suc, ''Nª CR "USO-0"'' AS estado, num_cr0 AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''Nª AISLADA'' AS estado, canalizado AS fecha FROM datos
                UNION ALL
                SELECT num_suc, ''Nª PEDESTAL'' AS estado, pedestal AS fecha FROM datos
            '
        ) AS sm(num_suc TEXT, estado TEXT, fecha TEXT)
        INNER JOIN public."work_packages" wp
        ON wp.subject = sm.num_suc
        ORDER BY wp.id, sm.estado
    ),
    actualizar AS (
        SELECT
            cv.id AS custom_value_id,
            d.fecha
        FROM custom_values cv
        INNER JOIN datos d
            ON cv.customized_id = d.work_package_id
        INNER JOIN custom_fields cf
            ON cf.id = cv.custom_field_id AND cf.name = d.estado
    )
    UPDATE custom_values cv
    SET value = actualizar.fecha
    FROM actualizar
    WHERE cv.id = actualizar.custom_value_id;
    """
    query12 = """
UPDATE public.work_packages wp
SET project_id = p.id
FROM public.custom_values cv
JOIN public.custom_fields cf ON cf.id = cv.custom_field_id
JOIN public.projects p ON p.name = cv.value
WHERE
  wp.id = cv.customized_id
  AND wp.type_id = 8
  AND cf.name = 'PARTNER';
    """
    query13 = """
UPDATE public.work_packages wp
SET project_id = p.id
FROM public.custom_values cv
JOIN public.custom_fields cf ON cf.id = cv.custom_field_id
JOIN public.projects p ON p.name = cv.value || ' PETICIONES'
WHERE
  wp.id = cv.customized_id
  AND wp.type_id = 9
  AND cf.name = 'PARTNER';
    """
    query14 = """
WITH
-- 1. ID del campo 'FECHA POSIBLE ANULACIÓN'
anulacion_field AS (
  SELECT id AS custom_field_id_anulacion
  FROM public.custom_fields
  WHERE name = 'FECHA POSIBLE ANULACIÓN'
),

-- 2. Obtener el estado (nombre del campo que contiene la fecha base) por customized_id
estado_nombre_por_customized AS (
  SELECT customized_id, value AS estado_nombre
  FROM public.custom_values
  WHERE custom_field_id = 136
),

-- 3. Obtener el ID del campo que tiene el nombre igual al estado
estado_field_id AS (
  SELECT
    en.customized_id,
    en.estado_nombre,
    cf.id AS fecha_base_custom_field_id
  FROM estado_nombre_por_customized en
  JOIN public.custom_fields cf ON cf.name = en.estado_nombre
),

-- 4. Obtener la fecha base (value del campo con nombre igual al estado)
fecha_base AS (
  SELECT
    ef.customized_id,
    ef.estado_nombre,
    ef.fecha_base_custom_field_id,
    cv.value::date AS fecha_base
  FROM estado_field_id ef
  JOIN public.custom_values cv
    ON cv.customized_id = ef.customized_id AND cv.custom_field_id = ef.fecha_base_custom_field_id
),

-- 5. Obtener los días a sumar desde tabla estado
fecha_con_dias AS (
  SELECT
    fb.customized_id,
    fb.fecha_base,
    e.diasum
  FROM fecha_base fb
  JOIN temp.estado e ON fb.estado_nombre = e.estado
),

-- 6. Obtener el ID del campo a actualizar ('FECHA POSIBLE ANULACIÓN')
target AS (
  SELECT
    fcd.customized_id,
    a.custom_field_id_anulacion,
    TO_CHAR(fcd.fecha_base + fcd.diasum * INTERVAL '1 day', 'YYYY-MM-DD') AS nueva_fecha
  FROM fecha_con_dias fcd
  CROSS JOIN anulacion_field a
)
-- 7. Actualizar el campo con la nueva fecha
UPDATE public.custom_values cv
SET value = t.nueva_fecha
FROM target t
WHERE cv.customized_id = t.customized_id
  AND cv.custom_field_id = t.custom_field_id_anulacion;
    """
    query15 = """
INSERT INTO relations (from_id, to_id, relation_type)
SELECT
  wp9.id AS from_id,
  wp8.id AS to_id,
  'relates' AS relation_type
FROM work_packages wp9
JOIN custom_values cv90
  ON wp9.id = cv90.customized_id
  AND cv90.customized_type = 'WorkPackage'
  AND cv90.custom_field_id = 90
  AND cv90.value IS NOT NULL
JOIN work_packages wp8
  ON wp8.type_id = 8
  AND wp8.subject = cv90.value
WHERE wp9.type_id = 9
  AND NOT EXISTS (
    SELECT 1
    FROM relations r
    WHERE
      (r.from_id = wp9.id AND r.to_id = wp8.id)
      OR
      (r.from_id = wp8.id AND r.to_id = wp9.id)
  );
    """

    query16 = """
INSERT INTO custom_values (customized_type, customized_id, custom_field_id, value)
SELECT
  'WorkPackage',
  wp.id,
  71,
  NULL
FROM work_packages wp
LEFT JOIN custom_values cv
  ON cv.customized_type = 'WorkPackage'
  AND cv.customized_id = wp.id
  AND cv.custom_field_id = 71
WHERE wp.type_id = 8
  AND cv.id IS NULL;
    """

    query17 = r"""
UPDATE custom_values cv
SET value = REGEXP_REPLACE(p.name, '\s*PETICIONES\s*', '', 'gi')
FROM work_packages wp
JOIN projects p ON wp.project_id = p.id
WHERE cv.customized_type = 'WorkPackage'
  AND cv.customized_id = wp.id
  AND cv.custom_field_id = 71
  AND wp.type_id = 8
  AND (cv.value IS NULL OR cv.value = '');
    """

    query18 = """
INSERT INTO custom_values (customized_type, customized_id, custom_field_id, value)
SELECT 'WorkPackage', wp.id, 139, NULL
FROM work_packages wp
WHERE wp.type_id = 8
  AND NOT EXISTS (
    SELECT 1 FROM custom_values cv
    WHERE cv.customized_type = 'WorkPackage'
      AND cv.customized_id = wp.id
      AND cv.custom_field_id = 139
  );
    """

    query19 = """
UPDATE custom_values cv139
SET value = sub.new_value
FROM (
  SELECT
    wp.id AS wp_id,
    CASE
      WHEN cv85.value IS NOT NULL AND cv10.value IS NULL THEN '57'
      WHEN cv10.value IS NOT NULL AND cv85.value IS NULL THEN '56'
      WHEN cv85.value IS NULL AND cv10.value IS NULL THEN '93'
      ELSE '58'  -- caso imposible, lo puedes controlar con un SELECT previo si quieres
    END AS new_value
  FROM work_packages wp
  LEFT JOIN custom_values cv85
    ON cv85.customized_id = wp.id AND cv85.customized_type = 'WorkPackage' AND cv85.custom_field_id = 85
  LEFT JOIN custom_values cv10
    ON cv10.customized_id = wp.id AND cv10.customized_type = 'WorkPackage' AND cv10.custom_field_id = 10
  WHERE wp.type_id = 8
) AS sub
WHERE cv139.customized_id = sub.wp_id
  AND cv139.customized_type = 'WorkPackage'
  AND cv139.custom_field_id = 139;
    """

    query20 = """
UPDATE work_packages wp8
SET project_id = parent_projects.parent_id
FROM work_packages wp9
JOIN custom_values cv ON cv.customized_type = 'WorkPackage'
                     AND cv.customized_id = wp9.id
                     AND cv.custom_field_id = 90
                     AND cv.value IS NOT NULL
JOIN work_packages wp_target ON wp_target.type_id = 8
                            AND wp_target.subject = cv.value
JOIN relations r ON r.from_id = wp9.id AND r.to_id = wp_target.id
JOIN projects p9 ON p9.id = wp9.project_id
JOIN projects parent_projects ON parent_projects.id = p9.parent_id
WHERE wp8.id = wp_target.id
  AND wp9.type_id = 9;
    """


    # Ejecutar la consulta
    cursor.execute(query)
    cursor.execute(query2)
    cursor.execute(query3)
    cursor.execute(query4)
    cursor.execute(query5)
    cursor.execute(query6)
    cursor.execute(query7)
    cursor.execute(query8)
    cursor.execute(query9)
    cursor.execute(query10)
    cursor.execute(query11)
    cursor.execute(query12)
    cursor.execute(query13)
    cursor.execute(query14)
    cursor.execute(query15)
    cursor.execute(query16)
    cursor.execute(query17)
    cursor.execute(query18)
    cursor.execute(query19)
    cursor.execute(query20)
    # Confirmar la transacción
    connection.commit()

    print("Consulta ejecutada correctamente")

except Exception as e:
    print(f"Error al ejecutar la consulta: {e}")
    print(traceback.format_exc())
    if connection:
        connection.rollback()

finally:
    # Cerrar la conexión
    if cursor:
        cursor.close()
    if connection:
        connection.close()
