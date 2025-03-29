import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from src.models import (
    Base, TipoUsuario, TipoNota, Paises, Usuario, Hashtags, NotasXUsuario,
    HashtagsXNotas, Empresas, EmpresasXNota, TipoEvento,
    EventosFinancierosXEmpresa, CotizacionXEmpresa
)

class TestModels(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create an in-memory SQLite database for testing
        cls.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(cls.engine)
        Session = sessionmaker(bind=cls.engine)
        cls.session = Session()

    def setUp(self):
        # Create test data
        # Tipo Usuario
        self.tipo_usuario = TipoUsuario(
            cod_tipo_usuario=1,
            descripcion='Influencer'
        )
        self.session.add(self.tipo_usuario)

        # Tipo Nota
        self.tipo_nota = TipoNota(
            cod_tipo_nota=1,
            descripcion='texto'
        )
        self.session.add(self.tipo_nota)

        # País
        self.pais = Paises(
            cod_pais='AR',
            descripcion='Argentina'
        )
        self.session.add(self.pais)

        # Usuario
        self.usuario = Usuario(
            nombre='Test User',
            handle='@testuser',
            cod_tipo_usuario=1,
            verificado=True,
            seguidores=1000,
            cod_pais='AR',
            idioma_principal='es',
            score_credibilidad=0.8
        )
        self.session.add(self.usuario)

        # Hashtag
        self.hashtag = Hashtags(
            texto='#test'
        )
        self.session.add(self.hashtag)

        # Empresa
        self.empresa = Empresas(
            nombre='Test Company',
            ticker='TEST',
            sector='Technology',
            capitalizacion_bursatil=1000000.0
        )
        self.session.add(self.empresa)

        # Tipo Evento
        self.tipo_evento = TipoEvento(
            nombre_evento='Resultados Trimestrales',
            descripcion='Publicación de resultados financieros'
        )
        self.session.add(self.tipo_evento)

        self.session.commit()

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def test_create_tipo_nota(self):
        """Test tipo_nota creation and validation"""
        tipo_nota = self.session.query(TipoNota).filter_by(descripcion='texto').first()
        self.assertIsNotNone(tipo_nota)
        self.assertEqual(tipo_nota.descripcion, 'texto')

        # Test creating additional tipos
        tipo_imagen = TipoNota(
            cod_tipo_nota=2,
            descripcion='imagen'
        )
        self.session.add(tipo_imagen)
        self.session.commit()
        
        self.assertEqual(self.session.query(TipoNota).count(), 2)

    def test_create_usuario(self):
        """Test usuario creation and relationships"""
        usuario = self.session.query(Usuario).filter_by(handle='@testuser').first()
        self.assertIsNotNone(usuario)
        self.assertEqual(usuario.nombre, 'Test User')
        self.assertEqual(usuario.tipo_usuario.descripcion, 'Influencer')
        self.assertEqual(usuario.pais.descripcion, 'Argentina')

    def test_create_nota(self):
        """Test nota creation and relationships"""
        nota = NotasXUsuario(
            contenido='Test content',
            fecha_publicacion=datetime.utcnow(),
            id_usuario=self.usuario.id_usuario,
            cod_tipo_nota=self.tipo_nota.cod_tipo_nota,
            url_nota='https://test.com',
            engagement_total=100,
            score_analisis_sentimiento_nlp=0.5,
            sentimiento='positivo',
            score_sentimiento=0.8
        )
        self.session.add(nota)
        self.session.commit()

        # Test relationships
        self.assertEqual(nota.usuario.handle, '@testuser')
        self.assertEqual(nota.tipo_nota.descripcion, 'texto')
        
        # Add hashtag to nota
        hashtag_nota = HashtagsXNotas(
            id_nota=nota.id_nota,
            id_hashtag=self.hashtag.id_hashtag,
            frecuencia=1
        )
        self.session.add(hashtag_nota)
        self.session.commit()

        self.assertEqual(len(nota.hashtags), 1)
        self.assertEqual(nota.hashtags[0].hashtag.texto, '#test')

    def test_create_empresa_nota(self):
        """Test empresa-nota relationship and impact tracking"""
        nota = NotasXUsuario(
            contenido='Test company mention',
            fecha_publicacion=datetime.utcnow(),
            id_usuario=self.usuario.id_usuario,
            cod_tipo_nota=self.tipo_nota.cod_tipo_nota
        )
        self.session.add(nota)
        self.session.commit()

        empresa_nota = EmpresasXNota(
            id_nota=nota.id_nota,
            id_empresa=self.empresa.id_empresa,
            fuente_extraccion='test_model',
            tipo_mencion='directa',
            contexto='Mentioned in test',
            impacto_calculado=2.5,
            tipo_impacto='positivo',
            tiempo_reaccion=30.0
        )
        self.session.add(empresa_nota)
        self.session.commit()

        self.assertEqual(len(nota.empresas), 1)
        self.assertEqual(nota.empresas[0].empresa.nombre, 'Test Company')
        self.assertEqual(nota.empresas[0].impacto_calculado, 2.5)

    def test_create_evento_financiero(self):
        """Test financial event creation and relationships"""
        evento = EventosFinancierosXEmpresa(
            id_empresa=self.empresa.id_empresa,
            id_tipo_evento=self.tipo_evento.id_tipo_evento,
            fecha_evento=datetime.utcnow(),
            descripcion='Test event',
            impacto_esperado='positivo'
        )
        self.session.add(evento)
        self.session.commit()

        self.assertEqual(evento.empresa.nombre, 'Test Company')
        self.assertEqual(evento.tipo_evento.nombre_evento, 'Resultados Trimestrales')

    def test_create_cotizacion(self):
        """Test stock price data creation and relationships"""
        cotizacion = CotizacionXEmpresa(
            id_empresa=self.empresa.id_empresa,
            fecha=datetime.utcnow(),
            precio_apertura=100.0,
            precio_cierre=105.0,
            precio_max=106.0,
            precio_min=99.0,
            volumen_operado=1000000,
            variacion_porcentaje=5.0
        )
        self.session.add(cotizacion)
        self.session.commit()

        self.assertEqual(cotizacion.empresa.nombre, 'Test Company')
        self.assertEqual(cotizacion.variacion_porcentaje, 5.0)

    def test_constraints(self):
        """Test model constraints"""
        # Test unique constraint on hashtag
        duplicate_hashtag = Hashtags(texto='#test')
        with self.assertRaises(Exception):
            self.session.add(duplicate_hashtag)
            self.session.commit()

        # Test nullable constraints
        invalid_usuario = Usuario(
            handle='@testinvalid'  # Missing required nombre
        )
        with self.assertRaises(Exception):
            self.session.add(invalid_usuario)
            self.session.commit()

        # Test nota without required tipo_nota
        invalid_nota = NotasXUsuario(
            contenido='Test content',
            fecha_publicacion=datetime.utcnow(),
            id_usuario=self.usuario.id_usuario
            # Missing required cod_tipo_nota
        )
        with self.assertRaises(Exception):
            self.session.add(invalid_nota)
            self.session.commit()

    def test_cascading_delete(self):
        """Test cascading delete behavior"""
        # Create a nota with relationships
        nota = NotasXUsuario(
            contenido='Test cascade',
            fecha_publicacion=datetime.utcnow(),
            id_usuario=self.usuario.id_usuario,
            cod_tipo_nota=self.tipo_nota.cod_tipo_nota
        )
        self.session.add(nota)
        self.session.commit()

        hashtag_nota = HashtagsXNotas(
            id_nota=nota.id_nota,
            id_hashtag=self.hashtag.id_hashtag,
            frecuencia=1
        )
        self.session.add(hashtag_nota)
        self.session.commit()

        # Delete the nota and verify relationships are properly handled
        self.session.delete(nota)
        self.session.commit()

        # Verify hashtag still exists but relationship is gone
        hashtag = self.session.query(Hashtags).filter_by(texto='#test').first()
        self.assertIsNotNone(hashtag)
        hashtag_rel = self.session.query(HashtagsXNotas).filter_by(id_nota=nota.id_nota).first()
        self.assertIsNone(hashtag_rel)

if __name__ == '__main__':
    unittest.main()