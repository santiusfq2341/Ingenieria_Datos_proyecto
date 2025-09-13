-- Crear la base de datos
CREATE DATABASE IF NOT EXISTS retenciones_mensuales;

-- Usar la base de datos
USE retenciones_mensuales;

-- Crear la tabla
CREATE TABLE IF NOT EXISTS retenciones (
    no_secuencial_retencion CHAR(6) PRIMARY KEY,   -- 6 dígitos como string, clave primaria
    id_cliente VARCHAR(21) NOT NULL,               -- 21 dígitos
    fecha_emision DATE NOT NULL,                   -- tipo fecha
    cod_retencion CHAR(5) NOT NULL,                -- código retención, 5 caracteres
    base_imponible FLOAT(9,2) NOT NULL,            -- hasta 9 dígitos con 2 decimales
    monto_retencion FLOAT(9,2) NOT NULL            -- hasta 9 dígitos con 2 decimales
);



-- Crear la tabla
CREATE TABLE IF NOT EXISTS info_clientes (
    id_cliente VARCHAR(21) PRIMARY KEY,              -- 21 dígitos, clave primaria
    cod_tipo_id VARCHAR(2) NOT NULL,       -- 6 dígitos como string
    cod_provincia VARCHAR(2) NOT NULL                 
    
);
