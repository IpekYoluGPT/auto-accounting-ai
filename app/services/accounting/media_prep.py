"""
Media normalization helpers for Gemini document processing.
"""

from __future__ import annotations

from io import BytesIO

from PIL import Image, ImageOps, UnidentifiedImageError
from pydantic import BaseModel, Field

from app.utils.logging import get_logger

logger = get_logger(__name__)

_MAX_IMAGE_DIMENSION = 4096


class PreparedMedia(BaseModel):
    media_bytes: bytes
    mime_type: str
    width: int | None = None
    height: int | None = None
    warnings: list[str] = Field(default_factory=list)



def prepare_media(media_bytes: bytes, mime_type: str) -> PreparedMedia:
    if mime_type == "application/pdf":
        return PreparedMedia(media_bytes=media_bytes, mime_type=mime_type)

    if not mime_type.startswith("image/"):
        return PreparedMedia(
            media_bytes=media_bytes,
            mime_type=mime_type,
            warnings=[f"Unsupported media MIME type {mime_type}; using original bytes."],
        )

    try:
        with Image.open(BytesIO(media_bytes)) as image:
            orientation = image.getexif().get(274, 1)
            normalized = ImageOps.exif_transpose(image)
            width, height = normalized.size
            resized = False

            if max(width, height) > _MAX_IMAGE_DIMENSION:
                normalized.thumbnail((_MAX_IMAGE_DIMENSION, _MAX_IMAGE_DIMENSION))
                resized = True

            output_format = "PNG" if mime_type == "image/png" else "JPEG"
            output_mime = "image/png" if output_format == "PNG" else "image/jpeg"

            if output_format == "JPEG" and normalized.mode not in ("RGB", "L"):
                normalized = normalized.convert("RGB")
            elif output_format == "PNG" and normalized.mode == "P":
                normalized = normalized.convert("RGBA")

            width, height = normalized.size
            warnings: list[str] = []
            needs_reencode = orientation not in (None, 1) or resized or output_mime != mime_type
            if not needs_reencode:
                return PreparedMedia(
                    media_bytes=media_bytes,
                    mime_type=mime_type,
                    width=width,
                    height=height,
                )

            buffer = BytesIO()
            save_kwargs = {"format": output_format}
            if output_format == "JPEG":
                save_kwargs.update({"quality": 95, "optimize": True})
            normalized.save(buffer, **save_kwargs)

            if orientation not in (None, 1):
                warnings.append("Image was rotated using EXIF orientation.")
            if resized:
                warnings.append("Large image was downscaled for Gemini stability.")

            return PreparedMedia(
                media_bytes=buffer.getvalue(),
                mime_type=output_mime,
                width=width,
                height=height,
                warnings=warnings,
            )
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        logger.info("Image normalization skipped; using original bytes: %s", exc)
        return PreparedMedia(
            media_bytes=media_bytes,
            mime_type=mime_type,
            warnings=[f"Image normalization skipped: {exc}"],
        )
