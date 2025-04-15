from models import engine, SessionLocal, init_db, Usuario, TipoUsuario

def main():
    # Inicializa las tablas en la base de datos si aún no existen.
    init_db()

    # Crear una sesión para interactuar con la base de datos.
    session = SessionLocal()
    try:
        # Ejemplo: Insertar un registro en TipoUsuario.
        admin_tipo = TipoUsuario(cod_tipo_usuario=1, descripcion="Administrador")
        session.add(admin_tipo)
        session.commit()

        # Crear un nuevo usuario utilizando el tipo creado.
        nuevo_usuario = Usuario(
            nombre="Juan Pérez",
            handle="juanp",
            cod_tipo_usuario=admin_tipo.cod_tipo_usuario,  # Relaciona el usuario con el tipo
            verificado=True,
            seguidores=1200,
            cod_pais="AR",  # Asegúrate de que el código de país exista en la tabla Paises o ajusta según tu modelo
            idioma_principal="es",
            score_credibilidad=0.95
        )
        session.add(nuevo_usuario)
        session.commit()

        # Realizar una consulta para verificar que se insertó correctamente.
        usuario_db = session.query(Usuario).filter_by(handle="juanp").first()
        if usuario_db:
            print(f"Usuario creado: {usuario_db.nombre} con handle {usuario_db.handle}")
        else:
            print("No se encontró el usuario.")

    except Exception as e:
        session.rollback()
        print("Ocurrió un error:", e)
    finally:
        session.close()

if __name__ == '__main__':
    main()
