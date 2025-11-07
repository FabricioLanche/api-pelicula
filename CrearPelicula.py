import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    # ========================================================================
    # ESTRUCTURA TRY-EXCEPT: Manejo de errores para capturar excepciones
    # ========================================================================
    try:
        # ====================================================================
        # ENTRADA: Parseo y validación de datos
        # ====================================================================
        # Log del evento recibido en formato estándar INFO
        log_evento = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Evento recibido",
                "event": event
            }
        }
        print(json.dumps(log_evento))
        
        # Parseo del body (API Gateway envía body como string JSON)
        body = json.loads(event['body'])
        tenant_id = body['tenant_id']
        pelicula_datos = body['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Log de datos parseados exitosamente
        log_parseo = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Datos parseados correctamente",
                "tenant_id": tenant_id,
                "nombre_tabla": nombre_tabla
            }
        }
        print(json.dumps(log_parseo))
        
        # ====================================================================
        # PROCESO: Creación del registro de película
        # ====================================================================
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        # Conexión a DynamoDB y guardado del registro
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # ====================================================================
        # SALIDA: Log de ejecución exitosa en formato estándar INFO
        # ====================================================================
        log_exito = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "pelicula": pelicula,
                "dynamodb_response": {
                    "HTTPStatusCode": response['ResponseMetadata']['HTTPStatusCode'],
                    "RequestId": response['ResponseMetadata']['RequestId']
                }
            }
        }
        print(json.dumps(log_exito))
        
        # Respuesta exitosa
        return {
            'statusCode': 200,
            'body': json.dumps({
                'mensaje': 'Película creada exitosamente',
                'pelicula': pelicula
            })
        }
    
    # ========================================================================
    # MANEJO DE ERRORES: Captura de excepciones específicas y generales
    # ========================================================================
    except KeyError as e:
        # Error por campo faltante en el body
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Campo requerido faltante",
                "error": f"KeyError: {str(e)}",
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Campo requerido faltante',
                'detalle': str(e)
            })
        }
    
    except json.JSONDecodeError as e:
        # Error al parsear el JSON del body
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error al parsear JSON",
                "error": f"JSONDecodeError: {str(e)}",
                "body_recibido": event.get('body', '')
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Formato JSON inválido',
                'detalle': str(e)
            })
        }
    
    except Exception as e:
        # Error general no esperado
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error inesperado",
                "error": f"{type(e).__name__}: {str(e)}",
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Error interno del servidor',
                'detalle': str(e)
            })
        }
