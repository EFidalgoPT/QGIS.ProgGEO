import os
import json
import psycopg2
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from shapely.geometry import mapping  # Substitui fiona.mapping
import fiona
from fiona.crs import CRS  # Substitui fiona.crs.from_epsg

# 1. Conexão com a base de dados
def connect_to_postgis(config_path):
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Arquivo config.json não encontrado no caminho: {os.path.abspath(config_path)}")

        with open(config_path, 'r') as file:
            config = json.load(file)

        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        print("Conexão com o PostgreSQL realizada com sucesso.")
        return conn
    except Exception as e:
        raise RuntimeError(f"Erro ao conectar ao PostgreSQL: {e}")

# 2. Listar as missões disponíveis
def list_missions(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, local FROM missoes;")
        missions = cursor.fetchall()
        if not missions:
            raise ValueError("Nenhuma missão encontrada na tabela 'missoes'.")
    return missions

# 3. Obter dados para a missão selecionada
def get_mission_data(conn, mission_id):
    with conn.cursor() as cursor:
        # Obter as coordenadas das fotos
        cursor.execute("""
        SELECT ST_X(geom), ST_Y(geom)
        FROM drone_mission
        WHERE mission_id = %s;
        """, (mission_id,))
        photo_coords = cursor.fetchall()
        if not photo_coords:
            raise ValueError(f"Nenhuma coordenada encontrada para a missão {mission_id}.")
        print("Coordenadas: Check")  # Validação simplificada

        # Obter o distrito associado à missão
        cursor.execute("""
        SELECT local
        FROM missoes
        WHERE id = %s;
        """, (mission_id,))
        district_name = cursor.fetchone()
        if not district_name:
            raise ValueError(f"Nenhum distrito identificado para a missão {mission_id}.")
        district_name = district_name[0]

        # Criar polígono fictício para o distrito (substitua por dados reais, se disponíveis)
        district_polygon = Polygon([
            (-9.5, 38.7), (-9.4, 38.7), (-9.4, 38.8), (-9.5, 38.8), (-9.5, 38.7)
        ])

        print(f"Distrito: {district_name}")  # Validação simplificada
        1
        
    return photo_coords, district_polygon

# 4. Criar polígono das fotos e recortar com o distrito
def create_polygon(photo_coords, district_polygon):
    # Criar polígono das coordenadas das fotos
    if not photo_coords:
        raise ValueError("Não há coordenadas suficientes para criar o polígono das fotos.")
    
    photo_points = [Point(x, y) for x, y in photo_coords]
    photo_polygon = unary_union(photo_points).convex_hull

    # Validar se o polígono das fotos foi criado
    if photo_polygon.is_empty:
        print("Polígono das fotos: Erro")
        raise ValueError("O polígono das fotos está vazio ou inválido.")
    else:
        print("Polígono das fotos: Criado")

    # Interseção entre o polígono das fotos e o polígono do distrito
    clipped_polygon = district_polygon.intersection(photo_polygon)

    # Caso a interseção esteja vazia, usar o polígono original das fotos
    if clipped_polygon.is_empty:
        print("A interseção está vazia. Usando o polígono original das fotos.")
        return photo_polygon
    
    return clipped_polygon

# 5. Salvar o polígono como shapefile
def save_polygon_to_shapefile(polygon, output_path):
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'}
    }

    with fiona.open(output_path, 'w', driver='ESRI Shapefile',
                    crs=CRS.from_epsg(4326), schema=schema) as shp:  # Substitui fiona.crs.from_epsg
        shp.write({
            'geometry': mapping(polygon),  # Substitui fiona.mapping
            'properties': {'id': 1}
        })
    print(f"Shapefile salvo com sucesso em: {output_path}")

# 6. Função principal
def process_mission_to_shapefile(config_path, output_folder):
    try:
        conn = connect_to_postgis(config_path)
        try:
            # Listar missões disponíveis
            missions = list_missions(conn)
            print("Missões disponíveis:")
            for mission_id, local in missions:
                print(f"ID: {mission_id}, Local: {local}")

            # Perguntar ao usutilizador qual a missão deseja processar
            selected_id = int(input("Digite o ID da missão que deseja processar: "))
            if selected_id not in [mission[0] for mission in missions]:
                raise ValueError("ID da missão inválido.")

            # Obter dados da missão selecionada
            photo_coords, district_polygon = get_mission_data(conn, selected_id)

            # Criar o polígono
            clipped_polygon = create_polygon(photo_coords, district_polygon)

            # Alteração do destino para a pasta ShapeFiles
            output_path = os.path.join(output_folder, f"mission_{selected_id}_polygon.shp")
            save_polygon_to_shapefile(clipped_polygon, output_path)
        finally:
            conn.close()
            print("Conexão com o banco de dados encerrada.")
    except Exception as e:
        print(f"Erro durante o processamento: {e}")

# Execução do código
if __name__ == "__main__":
    # Configurações
    config_path = "C:/GeoSpacialDataBase/config.json"  # Caminho para o config.json
    output_folder = "C:/GeoSpacialDataBase/ShapeFiles"  # Nova pasta para salvar os shapefiles

    # Criar a pasta ShapeFiles se não existir
    os.makedirs(output_folder, exist_ok=True)

    # Processar a missão
    process_mission_to_shapefile(config_path, output_folder)
