#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para modelado automatizado de predicción de movimientos bursátiles por empresa.
Versión para ejecución en background con sistema de logging.
"""

import os
import sys
import time
import logging
import traceback
import warnings
from datetime import datetime

# Silenciar advertencias no críticas
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="Variables are collinear")
warnings.filterwarnings("ignore", message="Target scores need to be probabilities")

# Configurar matplotlib antes de importarlo
import matplotlib
matplotlib.use('Agg')  # Usar el backend Agg que es más estable para entornos sin GUI
matplotlib.rcParams['font.family'] = 'DejaVu Sans'  # Especificar una fuente que sabemos está instalada

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pycaret.classification import *

# Importaciones adicionales para las visualizaciones manuales
from sklearn.metrics import confusion_matrix, roc_curve, auc, precision_recall_curve, average_precision_score

# Configuración del sistema de logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"modelado_pycaret_{timestamp}.log")

# Configurar el logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Crear directorio para resultados
results_dir = f"resultados_{timestamp}"
if not os.path.exists(results_dir):
    os.makedirs(results_dir)
    os.makedirs(os.path.join(results_dir, "plots"))
    os.makedirs(os.path.join(results_dir, "models"))

# Función para registrar tiempo de ejecución
def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Iniciando {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Completado {func.__name__} en {end_time - start_time:.2f} segundos")
        return result
    return wrapper

@timeit
def cargar_datos():
    """Carga y realiza una exploración inicial del dataset."""
    try:
        df = pd.read_csv('data_set_integrado_modelo_final_futuro.csv')
        logger.info(f"Dataset cargado con éxito: {df.shape[0]} filas y {df.shape[1]} columnas")

        # Reemplazar label con label_t_plus_1
        if 'label_t_plus_1' in df.columns:
            logger.info("Reemplazando columna 'label' con 'label_t_plus_1'")
            if 'label' in df.columns:
                df = df.drop(columns=['label'])
            df = df.rename(columns={'label_t_plus_1': 'label'})
            logger.info("Columnas ajustadas correctamente")
        else:
            logger.warning("No se encontró la columna 'label_t_plus_1' en el dataset")
  
        # Guardar información básica sobre el dataset
        with open(os.path.join(results_dir, "info_dataset.txt"), "w") as f:
            f.write(f"Dimensiones del dataset: {df.shape}\n")
            f.write(f"Número de filas: {df.shape[0]}\n")
            f.write(f"Número de columnas: {df.shape[1]}\n\n")
            
            # Información sobre la variable objetivo
            label_counts = df['label'].value_counts()
            f.write("Distribución de la variable 'label':\n")
            f.write(f"{label_counts.to_string()}\n\n")
            
            # Información sobre empresas
            empresas = df['id_empresa'].unique()
            f.write(f"Número de empresas distintas: {len(empresas)}\n")
            f.write(f"IDs de empresas: {empresas.tolist()}\n")
        
        # Guardamos algunas visualizaciones iniciales
        plt.figure(figsize=(10, 6))
        sns.countplot(x='label', data=df)
        plt.title('Distribución de Clases en el Dataset Completo')
        plt.ylabel('Conteo')
        plt.xlabel('Clase')
        plt.savefig(os.path.join(results_dir, "plots", "distribucion_clases.png"))
        plt.close()
        
        plt.figure(figsize=(14, 8))
        sns.countplot(x='id_empresa', data=df)
        plt.title('Cantidad de Registros por Empresa')
        plt.ylabel('Conteo')
        plt.xlabel('ID de Empresa')
        plt.xticks(rotation=45)
        plt.savefig(os.path.join(results_dir, "plots", "registros_por_empresa.png"))
        plt.close()
        
        # Distribución de clases por empresa
        plt.figure(figsize=(16, 10))
        for i, emp in enumerate(empresas, 1):
            plt.subplot(2, (len(empresas)+1)//2, i)
            emp_df = df[df['id_empresa'] == emp]
            sns.countplot(x='label', data=emp_df)
            plt.title(f'Empresa {emp}')
            plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(results_dir, "plots", "clases_por_empresa.png"))
        plt.close()
        
        return df
    
    except Exception as e:
        logger.error(f"Error al cargar los datos: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

@timeit
def preparar_datos(df):
    """Limpia y prepara los datos para el modelado."""
    try:
        # Hacemos una copia para no modificar el original
        df_clean = df.copy()
        
        # 1. Eliminar filas sin etiqueta
        rows_before = df_clean.shape[0]
        df_clean = df_clean.dropna(subset=['label'])
        rows_after = df_clean.shape[0]
        logger.info(f"Filas eliminadas por falta de etiqueta: {rows_before - rows_after}")
        
        # 2. Verificar valores nulos
        null_counts = df_clean.isnull().sum()
        with open(os.path.join(results_dir, "valores_nulos.txt"), "w") as f:
            f.write("Valores nulos por columna:\n")
            f.write(f"{null_counts.to_string()}\n\n")
        
        # 3. Eliminar columnas con más del 50% de valores faltantes
        threshold = 0.5
        missing_ratio = df_clean.isnull().mean()
        cols_to_drop = missing_ratio[missing_ratio > threshold].index.tolist()
        if cols_to_drop:
            logger.info(f"Columnas eliminadas por tener más del 50% de valores nulos: {cols_to_drop}")
            df_clean = df_clean.drop(columns=cols_to_drop)
        
        # 4. Eliminar cualquier fila restante con valores faltantes
        rows_before = df_clean.shape[0]
        df_clean = df_clean.dropna()
        rows_after = df_clean.shape[0]
        logger.info(f"Filas eliminadas por valores nulos: {rows_before - rows_after}")
        
        # Guardar información sobre correlaciones
        numeric_cols = df_clean.select_dtypes(include=['int64', 'float64']).columns.tolist()
        numeric_cols = [col for col in numeric_cols if col != 'id_empresa' and col != 'id_cotizacion']
        
        # Calculamos la matriz de correlación
        corr_matrix = df_clean[numeric_cols].corr()
        
        # Visualizamos el mapa de calor de correlaciones
        plt.figure(figsize=(16, 14))
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=False, cmap='coolwarm', center=0, linewidths=0.5)
        plt.title('Matriz de Correlación de Variables Numéricas')
        plt.tight_layout()
        plt.savefig(os.path.join(results_dir, "plots", "matriz_correlacion.png"))
        plt.close()
        
        # Identificamos variables altamente correlacionadas
        high_corr_threshold = 0.9
        high_corr_pairs = []
        
        for i in range(len(numeric_cols)):
            for j in range(i+1, len(numeric_cols)):
                corr = abs(corr_matrix.iloc[i, j])
                if corr > high_corr_threshold:
                    high_corr_pairs.append((numeric_cols[i], numeric_cols[j], corr))
        
        with open(os.path.join(results_dir, "correlaciones_altas.txt"), "w") as f:
            if high_corr_pairs:
                f.write("Variables altamente correlacionadas (|r| > 0.9):\n")
                for var1, var2, corr in high_corr_pairs:
                    f.write(f"{var1} - {var2}: {corr:.4f}\n")
            else:
                f.write("No se encontraron pares de variables con correlación mayor a 0.9\n")
        
        return df_clean
    
    except Exception as e:
        logger.error(f"Error al preparar los datos: {str(e)}")
        logger.error(traceback.format_exc())
        return None

@timeit
def modelar_empresa(df, empresa_id):
    """Construye y evalúa modelos para una empresa específica."""
    try:
        logger.info(f"{'='*50}")
        logger.info(f"Modelado para la empresa {empresa_id}")
        logger.info(f"{'='*50}")
        
        # Filtramos solo los datos de esta empresa
        empresa_df = df[df['id_empresa'] == empresa_id].copy()
        logger.info(f"Número de registros para la empresa {empresa_id}: {empresa_df.shape[0]}")
        
        # Verificamos la distribución de clases
        label_counts = empresa_df['label'].value_counts()
        logger.info(f"Distribución de clases:\n{label_counts}")
        
        # Verificamos si hay suficientes datos
        if empresa_df.shape[0] < 30:
            logger.warning(f"ADVERTENCIA: La empresa {empresa_id} tiene muy pocos registros para modelar adecuadamente.")
            return None, None, None
        
        # Eliminamos columnas no necesarias para el modelado
        cols_to_drop = ['id_cotizacion', 'id_empresa']
        modelo_df = empresa_df.drop(columns=cols_to_drop)
        
        # Inicializamos PyCaret
        exp_name = f"Empresa_{empresa_id}"
        logger.info("Configurando ambiente PyCaret...")
        
        # Nos aseguramos de que no se muestre la UI de html
        s = setup(
            data=modelo_df,
            target='label',
            session_id=42,
            experiment_name=exp_name,
            normalize=True,
            transformation=True,
            ignore_features=['fecha', 'nombre'],
            html=False,
            verbose=False
        )
        
        # Guardamos la información del setup
        setup_df = pull()
        setup_df.to_csv(os.path.join(results_dir, f"setup_empresa_{empresa_id}.csv"))
        logger.info("Setup completado y guardado")
        
        # Comparamos modelos con límites de tiempo para no quedarnos atascados
        logger.info("Comparando modelos...")
        # Usamos n_select=3 para limitar a los 3 mejores modelos
        # Excluimos modelos que pueden tardar mucho y fold=3 para reducir tiempo
        top_models = compare_models(
            n_select=3, 
            fold=3,
            exclude=['gbc', 'lightgbm', 'catboost'], 
            sort='F1',
            verbose=False
        )
        
        # Verificar si hay modelos, ya que compare_models puede devolver lista vacía
        if not top_models:
            logger.warning(f"No se encontraron modelos viables para la empresa {empresa_id}. Posible desbalance extremo de clases.")
            return empresa_df, [], {}
            
        # Si top_models es un solo modelo y no una lista, lo convertimos a lista
        if not isinstance(top_models, list):
            top_models = [top_models]
        
        # Guardamos la comparación de modelos
        model_comp = pull()
        model_comp.to_csv(os.path.join(results_dir, f"comparacion_modelos_empresa_{empresa_id}.csv"))
        logger.info(f"Comparación de modelos completada. Top modelos: {[str(type(m).__name__) for m in top_models]}")
        
        # Creamos un diccionario para almacenar los resultados
        resultados = {}
        
        # Evaluamos cada uno de los modelos top
        logger.info("Evaluando modelos seleccionados...")
        for i, model in enumerate(top_models):
            model_name = str(type(model).__name__)
            logger.info(f"Evaluando {model_name}...")
            
            # Evaluamos el modelo (guardamos en lugar de mostrar)
            try:
                # Creamos un modelo ajustado (tuneado) con número limitado de iteraciones
                logger.info(f"Tuneando {model_name}...")
                tuned_model = tune_model(
                    model, 
                    n_iter=10,  # Limitamos a 10 iteraciones para controlar el tiempo
                    optimize='F1',
                    search_library='optuna',
                    choose_better=True,
                    verbose=False
                )
                
                # VISUALIZACIONES - Método alternativo que no depende de plot_model
                logger.info(f"Generando visualizaciones para {model_name}...")
                try:
                    # 1. Matriz de confusión - Creación manual
                    y_pred = predict_model(tuned_model)
                    conf_matrix = confusion_matrix(y_pred['y_true'], y_pred['prediction_label'])
                    
                    plt.figure(figsize=(10, 8))
                    sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues',
                               xticklabels=sorted(y_pred['y_true'].unique()),
                               yticklabels=sorted(y_pred['y_true'].unique()))
                    plt.title(f'Matriz de Confusión - {model_name} - Empresa {empresa_id}')
                    plt.ylabel('Etiqueta Verdadera')
                    plt.xlabel('Etiqueta Predicha')
                    confusion_path = os.path.join(results_dir, "plots", f"confusion_matrix_empresa_{empresa_id}_{model_name}.png")
                    plt.tight_layout()
                    plt.savefig(confusion_path, bbox_inches='tight', dpi=100)
                    plt.close()
                    logger.info(f"Matriz de confusión guardada en {confusion_path}")
                    
                    # 2. Curva ROC - Solo para modelos que tienen predict_proba
                    try:
                        # Verificar primero si el modelo tiene predict_proba
                        if hasattr(tuned_model, 'predict_proba'):
                            y_pred_proba = predict_model(tuned_model, raw_score=True)
                            
                            # Verificar que tenemos las columnas de probabilidad
                            prob_cols = [col for col in y_pred_proba.columns if col.startswith('prediction_score_')]
                            
                            if prob_cols:
                                plt.figure(figsize=(10, 8))
                                
                                # Para cada clase, calcular y dibujar ROC
                                classes = sorted(y_pred_proba['y_true'].unique())
                                colors = ['blue', 'red', 'green', 'purple', 'orange', 'brown']
                                
                                for i, class_label in enumerate(classes):
                                    class_index = list(classes).index(class_label)
                                    if class_index < len(prob_cols):  # Asegurarse de que hay suficientes columnas
                                        prob_col = prob_cols[class_index]
                                        
                                        # One-vs-rest ROC
                                        y_true_bin = (y_pred_proba['y_true'] == class_label).astype(int)
                                        if len(y_true_bin.unique()) > 1:  # Solo si hay ejemplos positivos y negativos
                                            fpr, tpr, _ = roc_curve(y_true_bin, y_pred_proba[prob_col])
                                            roc_auc = auc(fpr, tpr)
                                            
                                            color = colors[i % len(colors)]
                                            plt.plot(fpr, tpr, color=color, lw=2,
                                                     label=f'ROC clase {class_label} (AUC = {roc_auc:.2f})')
                                
                                plt.plot([0, 1], [0, 1], 'k--', lw=2)
                                plt.xlim([0.0, 1.0])
                                plt.ylim([0.0, 1.05])
                                plt.xlabel('Tasa de Falsos Positivos')
                                plt.ylabel('Tasa de Verdaderos Positivos')
                                plt.title(f'Curva ROC - {model_name} - Empresa {empresa_id}')
                                plt.legend(loc="lower right")
                                roc_path = os.path.join(results_dir, "plots", f"roc_empresa_{empresa_id}_{model_name}.png")
                                plt.tight_layout()
                                plt.savefig(roc_path, bbox_inches='tight', dpi=100)
                                plt.close()
                                logger.info(f"Curva ROC guardada en {roc_path}")
                        else:
                            # Para modelos sin predict_proba, crear un gráfico alternativo (como precisión/recall)
                            y_pred = predict_model(tuned_model)
                            unique_classes = sorted(y_pred['y_true'].unique())
                            
                            plt.figure(figsize=(10, 8))
                            
                            # Calculamos precisión y recall por clase
                            for i, class_label in enumerate(unique_classes):
                                y_true_binary = (y_pred['y_true'] == class_label).astype(int)
                                y_pred_binary = (y_pred['prediction_label'] == class_label).astype(int)
                                
                                precision = np.sum((y_true_binary == 1) & (y_pred_binary == 1)) / (np.sum(y_pred_binary == 1) + 1e-10)
                                recall = np.sum((y_true_binary == 1) & (y_pred_binary == 1)) / (np.sum(y_true_binary == 1) + 1e-10)
                                
                                plt.scatter(recall, precision, s=100, label=f'Clase {class_label}')
                            
                            plt.title(f'Precisión vs. Recall por Clase - {model_name} - Empresa {empresa_id}')
                            plt.xlabel('Recall')
                            plt.ylabel('Precisión')
                            plt.grid(True, linestyle='--', alpha=0.7)
                            plt.xlim([-0.05, 1.05])
                            plt.ylim([-0.05, 1.05])
                            plt.legend()
                            
                            pr_path = os.path.join(results_dir, "plots", f"precision_recall_empresa_{empresa_id}_{model_name}.png")
                            plt.tight_layout()
                            plt.savefig(pr_path, bbox_inches='tight', dpi=100)
                            plt.close()
                            logger.info(f"Gráfico Precisión-Recall guardado en {pr_path}")
                    except Exception as roc_err:
                        logger.warning(f"Error al generar curva ROC o alternativa: {str(roc_err)}")
                        # Si falla la ROC, intentamos otra visualización alternativa
                        try:
                            # Diagrama de barras con accuracy por clase
                            plt.figure(figsize=(10, 6))
                            class_metrics = {}
                            
                            # Calcular accuracy por clase
                            for class_label in sorted(y_pred['y_true'].unique()):
                                mask = (y_pred['y_true'] == class_label)
                                class_accuracy = np.mean(y_pred.loc[mask, 'y_true'] == y_pred.loc[mask, 'prediction_label'])
                                class_metrics[class_label] = class_accuracy
                            
                            # Crear gráfico
                            plt.bar(range(len(class_metrics)), list(class_metrics.values()), align='center')
                            plt.xticks(range(len(class_metrics)), list(class_metrics.keys()))
                            plt.title(f'Precisión por Clase - {model_name} - Empresa {empresa_id}')
                            plt.xlabel('Clase')
                            plt.ylabel('Precisión')
                            plt.ylim([0, 1])
                            
                            acc_path = os.path.join(results_dir, "plots", f"accuracy_por_clase_empresa_{empresa_id}_{model_name}.png")
                            plt.tight_layout()
                            plt.savefig(acc_path, bbox_inches='tight', dpi=100)
                            plt.close()
                            logger.info(f"Gráfico de precisión por clase guardado en {acc_path}")
                        except Exception as alt_err:
                            logger.warning(f"Error al generar visualización alternativa: {str(alt_err)}")
                    
                    # 3. Importancia de características - Método mejorado
                    try:
                        # Intentar obtener importancia de características de diferentes maneras
                        importance = None
                        features = get_config('X_train').columns.tolist()
                        
                        if hasattr(tuned_model, 'feature_importances_'):
                            importance = tuned_model.feature_importances_
                        elif hasattr(tuned_model, 'coef_'):
                            if tuned_model.coef_.ndim > 1:
                                importance = np.abs(tuned_model.coef_).sum(axis=0)
                            else:
                                importance = np.abs(tuned_model.coef_)
                        # Añadir soporte para modelos con importancia implícita
                        elif isinstance(tuned_model, (KNeighborsClassifier, LogisticRegression)):
                            # Para KNN y modelos lineales sin importancia explícita, usar permutación
                            from sklearn.inspection import permutation_importance
                            
                            # Obtener los datos de entrenamiento
                            X_train = get_config('X_train')
                            y_train = get_config('y_train')
                            
                            # Asegurarse de que X_train esté como array para permutation_importance
                            result = permutation_importance(tuned_model, X_train, y_train, 
                                                           n_repeats=5, random_state=42, n_jobs=-1)
                            importance = result.importances_mean
                        
                        # Si tenemos importancia y características, crear visualización
                        if importance is not None and len(features) == len(importance):
                            feature_importance = pd.DataFrame({
                                'Feature': features,
                                'Importance': importance
                            })
                            feature_importance = feature_importance.sort_values('Importance', ascending=False)
                            
                            # Mostrar solo las 15 características más importantes
                            top_n = min(15, len(feature_importance))
                            feature_importance = feature_importance.head(top_n)
                            
                            plt.figure(figsize=(12, 8))
                            sns.barplot(x='Importance', y='Feature', data=feature_importance)
                            plt.title(f'Importancia de Características - {model_name} - Empresa {empresa_id}')
                            plt.tight_layout()
                            feature_path = os.path.join(results_dir, "plots", f"feature_importance_empresa_{empresa_id}_{model_name}.png")
                            plt.savefig(feature_path, bbox_inches='tight', dpi=100)
                            plt.close()
                            logger.info(f"Importancia de características guardada en {feature_path}")
                        else:
                            # Si no podemos obtener la importancia, creamos una visualización de correlación
                            plt.figure(figsize=(12, 10))
                            X_train = get_config('X_train')
                            correlation = X_train.corrwith(get_config('y_train')).abs().sort_values(ascending=False)
                            
                            # Mostrar top 15 correlaciones
                            top_corr = correlation.head(15)
                            sns.barplot(x=top_corr.values, y=top_corr.index)
                            plt.title(f'Correlación con Variable Objetivo - {model_name} - Empresa {empresa_id}')
                            plt.xlabel('Correlación Absoluta')
                            plt.tight_layout()
                            
                            corr_path = os.path.join(results_dir, "plots", f"correlacion_objetivo_empresa_{empresa_id}_{model_name}.png")
                            plt.savefig(corr_path, bbox_inches='tight', dpi=100)
                            plt.close()
                            logger.info(f"Correlación con variable objetivo guardada en {corr_path}")
                    except Exception as feat_err:
                        logger.warning(f"Error al generar importancia de características: {str(feat_err)}")
                        # Creamos un gráfico alternativo si falla la importancia de características
                        try:
                            plt.figure(figsize=(10, 6))
                            # Distribución de predicciones vs. valores reales
                            sns.countplot(data=y_pred, x='prediction_label', hue='y_true')
                            plt.title(f'Distribución de Predicciones - {model_name} - Empresa {empresa_id}')
                            plt.xlabel('Clase Predicha')
                            plt.ylabel('Conteo')
                            plt.legend(title='Clase Real')
                            plt.tight_layout()
                            
                            dist_path = os.path.join(results_dir, "plots", f"distribucion_predicciones_{empresa_id}_{model_name}.png")
                            plt.savefig(dist_path, bbox_inches='tight', dpi=100)
                            plt.close()
                            logger.info(f"Distribución de predicciones guardada en {dist_path}")
                        except Exception as alt_feat_err:
                            logger.warning(f"Error al generar visualización alternativa de distribución: {str(alt_feat_err)}")
                except Exception as plot_error:
                    logger.warning(f"Error general al generar visualizaciones: {str(plot_error)}")
                    # Si fallan todas las visualizaciones específicas, intentamos con visualizaciones realmente básicas
                    try:
                        # Matriz de confusión simplificada
                        y_pred = predict_model(tuned_model)
                        labels = sorted(y_pred['y_true'].unique())
                        cm = confusion_matrix(y_pred['y_true'], y_pred['prediction_label'], labels=labels)
                        
                        plt.figure(figsize=(8, 6))
                        plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
                        plt.title(f'Matriz de Confusión - {model_name} - Empresa {empresa_id}')
                        plt.colorbar()
                        plt.tight_layout()
                        
                        basic_cm_path = os.path.join(results_dir, "plots", f"basic_cm_{empresa_id}_{model_name}.png")
                        plt.savefig(basic_cm_path)
                        plt.close()
                        logger.info(f"Matriz de confusión básica guardada en {basic_cm_path}")
                    except Exception as basic_err:
                        logger.error(f"No se pudo generar ni siquiera una visualización básica: {str(basic_err)}")
                        # En el peor caso, creamos una imagen que indica el error
                        plt.figure(figsize=(8, 6))
                        plt.text(0.5, 0.5, f"Error al generar visualizaciones\nModelo: {model_name}\nEmpresa: {empresa_id}", 
                                horizontalalignment='center', verticalalignment='center', fontsize=14, color='red')
                        plt.axis('off')
                        error_path = os.path.join(results_dir, "plots", f"error_visualizacion_{empresa_id}_{model_name}.png")
                        plt.savefig(error_path)
                        plt.close()
                        logger.error(f"Imagen de error guardada en {error_path}")
                
                # Guardamos el modelo
                model_path = os.path.join(results_dir, "models", f"modelo_{empresa_id}_{model_name}")
                logger.info(f"Guardando modelo en {model_path}...")
                save_model(tuned_model, model_path)
                
                resultados[model_name] = tuned_model
                
                # Guardamos las métricas detalladas
                try:
                    metrics = pull()
                    metrics.to_csv(os.path.join(results_dir, f"metricas_empresa_{empresa_id}_{model_name}.csv"))
                except Exception as metrics_error:
                    logger.warning(f"Error al guardar métricas: {str(metrics_error)}")
                
            except Exception as model_error:
                logger.error(f"Error procesando modelo {model_name}: {str(model_error)}")
                logger.error(traceback.format_exc())
                continue  # Continuamos con el siguiente modelo
        
        # Finalizamos
        logger.info(f"Modelado completo para la empresa {empresa_id}")
        
        # Retornamos el DataFrame de la empresa, los modelos y los resultados
        return empresa_df, top_models, resultados
    
    except Exception as e:
        logger.error(f"Error en modelado de empresa {empresa_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None, None

@timeit
def main():
    """Función principal que ejecuta todo el proceso."""
    try:
        start_time = time.time()
        logger.info("="*80)
        logger.info("INICIANDO PROCESO DE MODELADO AUTOMATIZADO")
        logger.info("="*80)
        
        # 1. Cargar datos
        df = cargar_datos()
        
        # 2. Preparar datos
        df_clean = preparar_datos(df)
        if df_clean is None:
            logger.error("Error en la preparación de datos. Abortando.")
            return
        
        # 3. Obtener lista de empresas
        empresas = df_clean['id_empresa'].unique()
        logger.info(f"Se procesarán {len(empresas)} empresas: {empresas}")
        
        # 4. Modelado por empresa
        resultados_por_empresa = {}
        
        for empresa_id in empresas:
            empresa_start_time = time.time()
            logger.info(f"Procesando empresa {empresa_id}...")
            
            try:
                empresa_df, modelos, resultados = modelar_empresa(df_clean, empresa_id)
                
                # Verificar si hay resultados válidos antes de almacenar
                if empresa_df is not None and modelos and resultados:
                    resultados_por_empresa[empresa_id] = {
                        'dataframe': empresa_df,
                        'modelos': modelos,
                        'resultados': resultados
                    }
                    
                empresa_end_time = time.time()
                logger.info(f"Empresa {empresa_id} procesada en {(empresa_end_time - empresa_start_time)/60:.2f} minutos")
                
            except Exception as e:
                logger.error(f"Error procesando empresa {empresa_id}: {str(e)}")
                logger.error(traceback.format_exc())
                continue  # Continuamos con la siguiente empresa
        
        # 5. Creamos un resumen comparativo si tenemos suficientes resultados
        if resultados_por_empresa:
            resumen_empresas = pd.DataFrame(columns=['Empresa', 'Mejor_Modelo', 'Accuracy', 'Precision', 'Recall', 'F1', 'Num_Registros'])
            
            for empresa_id, datos in resultados_por_empresa.items():
                if 'modelos' in datos and datos['modelos']:
                    try:
                        # Obtenemos el mejor modelo (el primero de la lista)
                        mejor_modelo = datos['modelos'][0]
                        modelo_nombre = str(type(mejor_modelo).__name__)
                        
                        # Obtenemos métricas 
                        metricas = None
                        metrics_path = os.path.join(results_dir, f"comparacion_modelos_empresa_{empresa_id}.csv")
                        try:
                            if os.path.exists(metrics_path):
                                metricas = pd.read_csv(metrics_path)
                            else:
                                logger.warning(f"Archivo de métricas no encontrado: {metrics_path}")
                                continue
                        except Exception as load_error:
                            logger.warning(f"No se pudieron cargar métricas para empresa {empresa_id}: {str(load_error)}")
                            continue
                            
                        if metricas is not None and not metricas.empty and 'Model' in metricas.columns:
                            # Verificar si el modelo existe en el dataframe de métricas
                            if modelo_nombre in metricas['Model'].values:
                                mejor_modelo_metricas = metricas[metricas['Model'] == modelo_nombre].iloc[0]
                                
                                # Verificamos que existen todas las columnas necesarias
                                required_cols = ['Accuracy', 'Prec. Macro', 'Recall Macro', 'F1 Macro']
                                if all(col in mejor_modelo_metricas.index for col in required_cols):
                                    # Añadimos al DataFrame
                                    resumen_row = {
                                        'Empresa': empresa_id,
                                        'Mejor_Modelo': modelo_nombre,
                                        'Accuracy': mejor_modelo_metricas['Accuracy'],
                                        'Precision': mejor_modelo_metricas['Prec. Macro'],
                                        'Recall': mejor_modelo_metricas['Recall Macro'],
                                        'F1': mejor_modelo_metricas['F1 Macro'],
                                        'Num_Registros': datos['dataframe'].shape[0]
                                    }
                                    
                                    # Usar concat en lugar de append (que está deprecado)
                                    resumen_empresas = pd.concat([resumen_empresas, pd.DataFrame([resumen_row])], ignore_index=True)
                                else:
                                    logger.warning(f"Faltan columnas requeridas en métricas para empresa {empresa_id}")
                            else:
                                logger.warning(f"Modelo {modelo_nombre} no encontrado en métricas para empresa {empresa_id}")
                        else:
                            logger.warning(f"Datos de métricas inválidos para empresa {empresa_id}")
                    except Exception as e:
                        logger.error(f"Error al crear resumen para empresa {empresa_id}: {str(e)}")
                        logger.error(traceback.format_exc())
            
            # Guardamos el resumen si hay datos
            if not resumen_empresas.empty:
                resumen_empresas.to_csv(os.path.join(results_dir, "resumen_modelos_por_empresa.csv"), index=False)
                logger.info("Resumen comparativo guardado")
                
                # Visualizamos las métricas por empresa
                try:
                    plt.figure(figsize=(14, 10))
                    
                    # Accuracy
                    plt.subplot(2, 2, 1)
                    sns.barplot(x='Empresa', y='Accuracy', data=resumen_empresas)
                    plt.title('Accuracy por Empresa')
                    plt.xticks(rotation=45)
                    
                    # Precision
                    plt.subplot(2, 2, 2)
                    sns.barplot(x='Empresa', y='Precision', data=resumen_empresas)
                    plt.title('Precision por Empresa')
                    plt.xticks(rotation=45)
                    
                    # Recall
                    plt.subplot(2, 2, 3)
                    sns.barplot(x='Empresa', y='Recall', data=resumen_empresas)
                    plt.title('Recall por Empresa')
                    plt.xticks(rotation=45)
                    
                    # F1
                    plt.subplot(2, 2, 4)
                    sns.barplot(x='Empresa', y='F1', data=resumen_empresas)
                    plt.title('F1-Score por Empresa')
                    plt.xticks(rotation=45)
                    
                    plt.tight_layout()
                    plt.savefig(os.path.join(results_dir, "plots", "metricas_por_empresa.png"))
                    plt.close()
                    
                    # Gráfico de comparación de modelos por empresa
                    plt.figure(figsize=(12, 8))
                    sns.countplot(x='Mejor_Modelo', data=resumen_empresas)
                    plt.title('Distribución de Mejores Modelos por Empresa')
                    plt.xticks(rotation=45)
                    plt.ylabel('Conteo')
                    plt.savefig(os.path.join(results_dir, "plots", "distribucion_modelos.png"))
                    plt.close()
                    
                    logger.info("Visualizaciones de resumen guardadas")
                except Exception as e:
                    logger.error(f"Error al crear visualizaciones de resumen: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                logger.warning("No se creó resumen comparativo: No hay suficientes datos válidos")
        else:
            logger.warning("No se creó resumen comparativo: No hay resultados por empresa")
        
        # 6. Conclusiones y resumen final
        end_time = time.time()
        total_time_mins = (end_time - start_time) / 60
        
        with open(os.path.join(results_dir, "conclusiones.txt"), "w") as f:
            f.write("RESUMEN DEL ANÁLISIS:\n")
            f.write("="*80 + "\n")
            f.write(f"Total de empresas analizadas: {len(resultados_por_empresa)}\n")
            f.write(f"Tiempo total de ejecución: {total_time_mins:.2f} minutos\n\n")
            
            if 'resumen_empresas' in locals() and not resumen_empresas.empty:
                mejor_empresa = resumen_empresas.iloc[resumen_empresas['Accuracy'].idxmax()]
                f.write(f"Empresa con mejor rendimiento: {mejor_empresa['Empresa']}\n")
                f.write(f"- Mejor modelo: {mejor_empresa['Mejor_Modelo']}\n")
                f.write(f"- Accuracy: {mejor_empresa['Accuracy']:.4f}\n")
                f.write(f"- F1-Score: {mejor_empresa['F1']:.4f}\n\n")
                
                # Modelos más frecuentes
                modelo_mas_comun = resumen_empresas['Mejor_Modelo'].mode()[0]
                f.write(f"Modelo más común entre todas las empresas: {modelo_mas_comun}\n\n")
                
                # Promedio de métricas
                f.write("Rendimiento promedio de los modelos:\n")
                f.write(f"- Accuracy promedio: {resumen_empresas['Accuracy'].mean():.4f}\n")
                f.write(f"- Precision promedio: {resumen_empresas['Precision'].mean():.4f}\n")
                f.write(f"- Recall promedio: {resumen_empresas['Recall'].mean():.4f}\n")
                f.write(f"- F1-Score promedio: {resumen_empresas['F1'].mean():.4f}\n\n")
            
            f.write("\nCONCLUSIONES:\n")
            f.write("="*80 + "\n")
            f.write("""
1. Hemos construido modelos de clasificación multiclase para predecir los movimientos 
   del mercado (SUBE, BAJA, MANTIENE) para cada empresa en el dataset.

2. Para cada empresa, hemos identificado las características más relevantes que 
   influyen en los movimientos de sus acciones.

3. Se han guardado los modelos entrenados para cada empresa, que pueden ser utilizados 
   para hacer predicciones futuras sobre nuevos datos.

4. Los resultados muestran diferentes niveles de predictibilidad entre las empresas, 
   lo que sugiere que algunos movimientos de mercado son más fáciles de predecir que otros.

5. Recomendaciones para mejorar los modelos:
   - Incorporar más datos históricos
   - Explorar técnicas de series temporales más avanzadas
   - Considerar variables macroeconómicas y noticias del sector
   - Implementar técnicas de balanceo de clases para mejorar la predicción de clases minoritarias
""")
            
        logger.info("="*80)
        logger.info("PROCESO COMPLETADO EXITOSAMENTE")
        logger.info(f"Tiempo total de ejecución: {total_time_mins:.2f} minutos")
        logger.info(f"Resultados guardados en: {os.path.abspath(results_dir)}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Error en el proceso principal: {str(e)}")
        logger.error(traceback.format_exc())
        
if __name__ == "__main__":
    main()