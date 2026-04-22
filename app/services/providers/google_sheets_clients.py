"""
Google Sheets and Drive client initialization helpers.
"""

from __future__ import annotations

import base64
import json

_gspread_client = None
_creds = None
_drive_service = None
_sheets_service = None
_oauth_creds = None
_oauth_drive_service = None
_oauth_sheets_service = None


def get_client(*, settings, logger, scopes):
    global _gspread_client, _creds
    if _gspread_client is not None:
        return _gspread_client
    if not settings.google_service_account_json:
        logger.debug("GOOGLE_SERVICE_ACCOUNT_JSON not set; Google Sheets disabled.")
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        raw_json = base64.b64decode(settings.google_service_account_json).decode("utf-8")
        creds_dict = json.loads(raw_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _creds = creds
        _gspread_client = gspread.authorize(creds)
        logger.info(
            "Google Sheets client initialised (service account: %s)",
            creds_dict.get("client_email", "?"),
        )
        return _gspread_client
    except Exception as exc:
        logger.error("Failed to initialise Google Sheets client: %s", exc, exc_info=True)
        return None


def get_drive_service(*, settings, logger, scopes, force_refresh: bool = False):
    global _drive_service
    if force_refresh:
        _drive_service = None
    if _drive_service is not None:
        return _drive_service
    get_client(settings=settings, logger=logger, scopes=scopes)
    if _creds is None:
        return None
    try:
        from googleapiclient.discovery import build

        _drive_service = build("drive", "v3", credentials=_creds, cache_discovery=False)
        logger.debug("Google Drive service initialised.")
        return _drive_service
    except Exception as exc:
        logger.error("Failed to initialise Drive service: %s", exc, exc_info=True)
        return None


def get_sheets_service(*, settings, logger, scopes):
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service
    get_client(settings=settings, logger=logger, scopes=scopes)
    if _creds is None:
        return None
    try:
        from googleapiclient.discovery import build

        _sheets_service = build("sheets", "v4", credentials=_creds, cache_discovery=False)
        logger.debug("Google Sheets API service initialised.")
        return _sheets_service
    except Exception as exc:
        logger.error("Failed to initialise Sheets API service: %s", exc, exc_info=True)
        return None


def get_oauth_creds(*, settings, logger, scopes):
    global _oauth_creds
    if _oauth_creds is not None:
        return _oauth_creds

    if (
        not settings.google_oauth_client_id
        or not settings.google_oauth_client_secret
        or not settings.google_oauth_refresh_token
    ):
        return None

    try:
        from google.oauth2.credentials import Credentials

        _oauth_creds = Credentials(
            token=None,
            refresh_token=settings.google_oauth_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=settings.google_oauth_client_id,
            client_secret=settings.google_oauth_client_secret,
            scopes=scopes,
        )
        logger.info("OAuth2 user credentials initialised for file creation.")
        return _oauth_creds
    except Exception as exc:
        logger.error("Failed to build OAuth2 credentials: %s", exc, exc_info=True)
        return None


def get_oauth_drive_service(*, settings, logger, scopes, force_refresh: bool = False):
    global _oauth_drive_service
    if force_refresh:
        _oauth_drive_service = None
    if _oauth_drive_service is not None:
        return _oauth_drive_service
    creds = get_oauth_creds(settings=settings, logger=logger, scopes=scopes)
    if creds is None:
        return None
    try:
        from googleapiclient.discovery import build

        _oauth_drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.debug("OAuth Drive service initialised.")
        return _oauth_drive_service
    except Exception as exc:
        logger.error("Failed to initialise OAuth Drive service: %s", exc, exc_info=True)
        return None


def get_oauth_sheets_service(*, settings, logger, scopes):
    global _oauth_sheets_service
    if _oauth_sheets_service is not None:
        return _oauth_sheets_service
    creds = get_oauth_creds(settings=settings, logger=logger, scopes=scopes)
    if creds is None:
        return None
    try:
        from googleapiclient.discovery import build

        _oauth_sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        logger.debug("OAuth Sheets API service initialised.")
        return _oauth_sheets_service
    except Exception as exc:
        logger.error("Failed to initialise OAuth Sheets service: %s", exc, exc_info=True)
        return None
