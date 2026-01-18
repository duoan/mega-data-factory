"""
Quality Assessment Model Inference

Inference code for quality assessment models.
"""

from io import BytesIO

import numpy as np
import torch
from PIL import Image

from .trainer import QualityAssessmentModel


class QualityAssessmentInference:
    """Inference wrapper for quality assessment models."""

    def __init__(self, model_path: str | None = None, model: QualityAssessmentModel | None = None, device: str = "cpu"):
        """Initialize inference.

        Args:
            model_path: Path to saved model
            model: Pre-loaded model (alternative to model_path)
            device: Device to use ('cpu' or 'cuda')
        """
        self.device = torch.device(device)

        if model is not None:
            self.model = model.to(self.device)
        elif model_path is not None:
            self.model = QualityAssessmentModel()
            self.model.load_state_dict(torch.load(model_path, map_location=self.device))
            self.model = self.model.to(self.device)
        else:
            raise ValueError("Either model_path or model must be provided")

        self.model.eval()

    def predict(self, image: np.ndarray) -> float:
        """Predict quality score for a single image.

        Args:
            image: Image array of shape (H, W, C) or (C, H, W)

        Returns:
            Quality score between 0.0 and 1.0
        """
        # Preprocess image
        if image.ndim == 3 and image.shape[2] == 3:
            # (H, W, C) -> (C, H, W)
            image = np.transpose(image, (2, 0, 1))

        # Normalize to [0, 1]
        if image.max() > 1.0:
            image = image / 255.0

        # Convert to tensor and add batch dimension
        image_tensor = torch.from_numpy(image).float().unsqueeze(0).to(self.device)

        with torch.no_grad():
            output = self.model(image_tensor)
            score = output.item()

        return float(score)

    def predict_from_bytes(self, image_bytes: bytes) -> float:
        """Predict quality score from image bytes.

        Args:
            image_bytes: Image bytes data

        Returns:
            Quality score between 0.0 and 1.0
        """
        img = Image.open(BytesIO(image_bytes))
        img_array = np.array(img)
        return self.predict(img_array)
