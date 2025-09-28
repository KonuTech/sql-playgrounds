# Apache Superset Configuration File
import os

# Secret key for signing cookies - MUST be set before anything else
SECRET_KEY = 'M5QwzH1CORRyE/gLMRIdmNGGyxj+tV0RfoLdSExzNqzZoUf/f+ehg3AS'

# Explicitly disable the default secret key check
PREVENT_UNSAFE_DEFAULT_SECRET_KEY = False

# Database configuration for Superset metadata - using pg8000 driver
SQLALCHEMY_DATABASE_URI = "postgresql+pg8000://admin:admin123@postgres:5432/superset"

# Redis configuration for caching
REDIS_HOST = "redis"
REDIS_PORT = 6379
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': f'redis://{REDIS_HOST}:{REDIS_PORT}/1'
}

# Feature flags
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Disable example data to avoid transaction conflicts
SUPERSET_LOAD_EXAMPLES = False

# Configure pg8000 for better compatibility with Superset
import pg8000
# Use default paramstyle (format) for SQLAlchemy compatibility

# SQLAlchemy configuration for pg8000 compatibility
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_recycle': 3600,
    'pool_pre_ping': True,
    'echo': False,
    'connect_args': {
        'application_name': 'superset'
    }
}

# Create psycopg2 mock to satisfy Superset's internal import requirements
import sys
from unittest.mock import MagicMock

# Mock psycopg2 modules that Superset tries to import
psycopg2_mock = MagicMock()
psycopg2_mock.__version__ = '2.9.10'
psycopg2_mock.extensions = MagicMock()
psycopg2_mock.extras = MagicMock()
psycopg2_mock.sql = MagicMock()

# Add mocks to sys.modules before any Superset imports
sys.modules['psycopg2'] = psycopg2_mock
sys.modules['psycopg2._psycopg'] = MagicMock()
sys.modules['psycopg2.extensions'] = psycopg2_mock.extensions
sys.modules['psycopg2.extras'] = psycopg2_mock.extras
sys.modules['psycopg2.sql'] = psycopg2_mock.sql

# Row limit for SQL queries
ROW_LIMIT = 5000

# Webserver configuration
WEBSERVER_THREADS = 4

# Security settings
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = ['superset.views.api', 'superset.security.views']
WTF_CSRF_TIME_LIMIT = None

# Additional CSRF configuration for API endpoints
CSRF_ENABLED = True
WTF_CSRF_CHECK_DEFAULT = True

# API and frontend CSRF handling
SUPERSET_WEBSERVER_TIMEOUT = 60
PUBLIC_ROLE_LIKE_GAMMA = True

# Session configuration for stability
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

# Use Redis for session storage to fix CSRF issues
SESSION_TYPE = 'redis'
SESSION_REDIS_HOST = REDIS_HOST
SESSION_REDIS_PORT = REDIS_PORT
SESSION_REDIS_DB = 2
SESSION_PERMANENT = False
SESSION_USE_SIGNER = True
SESSION_KEY_PREFIX = 'superset_session:'

# Additional configuration for SQL Lab stability
SQLLAB_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600

# Email configuration (optional)
SMTP_HOST = "localhost"
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = "admin@superset.com"
SMTP_PORT = 25
SMTP_MAIL_FROM = "admin@superset.com"

# Default language
BABEL_DEFAULT_LOCALE = "en"
BABEL_DEFAULT_FOLDER = "superset/translations"
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
}

# =============================================================================
# SUPERSET NATIVE LOGGING CONFIGURATION - For debugging and monitoring
# =============================================================================
import logging
from datetime import datetime

# Ensure log directory exists
LOG_DIR = '/app/logs'
os.makedirs(LOG_DIR, exist_ok=True)

# Superset-native logging configuration (this is what Superset actually uses)
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'simple': {
            'format': '%(asctime)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'superset_file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': f'{LOG_DIR}/superset.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 30,
            'formatter': 'detailed',
            'level': 'INFO'
        },
        'error_file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': f'{LOG_DIR}/superset_errors.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
            'formatter': 'detailed',
            'level': 'ERROR'
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'level': 'INFO'
        }
    },
    'loggers': {
        # Main Superset application logger
        'superset': {
            'handlers': ['superset_file', 'error_file', 'console'],
            'level': 'INFO',
            'propagate': False
        },
        # Superset-specific modules
        'superset.charts': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'superset.dashboards': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'superset.views': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'superset.sql_lab': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'superset.connectors': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        # Database and SQLAlchemy logs
        'sqlalchemy.engine': {
            'handlers': ['superset_file'],
            'level': 'WARNING',
            'propagate': False
        },
        'sqlalchemy.pool': {
            'handlers': ['superset_file'],
            'level': 'WARNING',
            'propagate': False
        },
        # Flask and security logs
        'flask': {
            'handlers': ['superset_file'],
            'level': 'WARNING',
            'propagate': False
        },
        'flask_appbuilder': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'flask_appbuilder.security': {
            'handlers': ['superset_file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        # Gunicorn web server logs
        'gunicorn': {
            'handlers': ['superset_file'],
            'level': 'INFO',
            'propagate': False
        },
        'gunicorn.error': {
            'handlers': ['error_file'],
            'level': 'ERROR',
            'propagate': False
        },
        'gunicorn.access': {
            'handlers': ['superset_file'],
            'level': 'INFO',
            'propagate': False
        }
    },
    'root': {
        'handlers': ['console'],
        'level': 'WARNING'
    }
}

# Enable logging and application insights
ENABLE_PROXY_FIX = True

# Log level for Flask-appbuilder
FAB_LOG_LEVEL = "INFO"

# Enable Flask debugging in development
FLASK_ENV = 'development'

# Startup logging function (called by Superset during initialization)
import logging.config
def configure_logging():
    """Configure Superset logging on startup"""
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger('superset.config')
    logger.info("=" * 80)
    logger.info("🚀 SUPERSET PERSISTENT LOGGING CONFIGURED")
    logger.info("=" * 80)
    logger.info(f"📁 Log Directory: {LOG_DIR}")
    logger.info(f"🗄️ Database: {SQLALCHEMY_DATABASE_URI.split('@')[1] if '@' in SQLALCHEMY_DATABASE_URI else 'configured'}")
    logger.info(f"🔑 Redis Cache: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"🔒 CSRF Protection: {WTF_CSRF_ENABLED}")
    logger.info(f"📊 Row Limit: {ROW_LIMIT}")
    logger.info("📋 Log Files:")
    logger.info(f"  • Application: superset.log")
    logger.info(f"  • Errors: superset_errors.log")
    logger.info("🚩 Feature Flags:")
    for flag, enabled in FEATURE_FLAGS.items():
        logger.info(f"  • {flag}: {'✅' if enabled else '❌'}")
    logger.info("=" * 80)

# Configure logging immediately
configure_logging()