import psycopg2
import traceback
from psycopg2 import sql

# Conexión a la base de datos PostgreSQL con los parámetros proporcionados
try:
    connection = psycopg2.connect(
        host="suc.insdosl.com",         # Host de la base de datos
        port=5433,                      # Puerto de la base de datos
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
        1, 
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
            TO_CHAR((jsonb_array_elements(se.cleaned_historial_estados_json::jsonb)->>'fecha')::timestamp,'YYYY-MM-DD') AS fecha
        FROM 
            public.work_packages wp
        JOIN escapex_data se ON wp.subject = se.num_suc
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
                            WHEN COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%POSTE%'') > 0 THEN ''SI''
                            ELSE ''NO''
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
                            WHEN COUNT(*) FILTER (WHERE sme.tipo_registro ILIKE ''%POSTE%'') > 0 THEN ''SI''
                            ELSE ''NO''
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
    SET value = REPLACE(REPLACE(value, 'SI', 't'), 'NO', 'f')
    WHERE value IN ('SI', 'NO') 
    AND custom_field_id = 59;
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
