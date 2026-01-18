"""
Quality Assessment Model Trainer

Trains models for visual degradations assessment.
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset


class ImageQualityDataset(Dataset):
    """Dataset for image quality assessment training."""

    def __init__(self, images: list[np.ndarray], labels: list[float]):
        """Initialize dataset.

        Args:
            images: List of image arrays
            labels: List of quality scores (0.0 to 1.0)
        """
        self.images = images
        self.labels = labels

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        image = torch.from_numpy(self.images[idx]).float()
        label = torch.tensor(self.labels[idx], dtype=torch.float32)
        return image, label


class QualityAssessmentModel(nn.Module):
    """Neural network model for quality assessment."""

    def __init__(self, input_channels: int = 3, num_classes: int = 1):
        """Initialize model.

        Args:
            input_channels: Number of input channels (3 for RGB)
            num_classes: Number of output classes (1 for regression)
        """
        super().__init__()
        # Simple CNN architecture - can be replaced with more sophisticated models
        self.conv1 = nn.Conv2d(input_channels, 32, kernel_size=3, padding=1)
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.conv3 = nn.Conv2d(64, 128, kernel_size=3, padding=1)
        self.pool = nn.AdaptiveAvgPool2d((1, 1))
        self.fc = nn.Linear(128, num_classes)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = self.relu(self.conv1(x))
        x = self.relu(self.conv2(x))
        x = self.relu(self.conv3(x))
        x = self.pool(x)
        x = x.view(x.size(0), -1)
        x = self.fc(x)
        x = self.sigmoid(x)  # Output between 0 and 1
        return x


class QualityAssessmentTrainer:
    """Trainer for quality assessment models."""

    def __init__(self, model: nn.Module | None = None, device: str = "cpu"):
        """Initialize trainer.

        Args:
            model: Optional pre-initialized model
            device: Device to use ('cpu' or 'cuda')
        """
        self.device = torch.device(device)
        if model is None:
            self.model = QualityAssessmentModel().to(self.device)
        else:
            self.model = model.to(self.device)
        self.criterion = nn.MSELoss()
        self.optimizer = None

    def train(self, train_loader: DataLoader, epochs: int = 10, lr: float = 0.001):
        """Train the model.

        Args:
            train_loader: DataLoader for training data
            epochs: Number of training epochs
            lr: Learning rate
        """
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)
        self.model.train()

        for epoch in range(epochs):
            total_loss = 0.0
            for batch_idx, (images, labels) in enumerate(train_loader):
                images = images.to(self.device)
                labels = labels.to(self.device)

                self.optimizer.zero_grad()
                outputs = self.model(images)
                loss = self.criterion(outputs.squeeze(), labels)
                loss.backward()
                self.optimizer.step()

                total_loss += loss.item()

            avg_loss = total_loss / len(train_loader)
            print(f"Epoch {epoch + 1}/{epochs}, Loss: {avg_loss:.4f}")

    def save(self, model_path: str):
        """Save trained model.

        Args:
            model_path: Path to save the model
        """
        import os

        os.makedirs(os.path.dirname(model_path) if os.path.dirname(model_path) else ".", exist_ok=True)
        torch.save(self.model.state_dict(), model_path)
        print(f"Model saved to {model_path}")

    def load(self, model_path: str):
        """Load trained model.

        Args:
            model_path: Path to the saved model
        """
        self.model.load_state_dict(torch.load(model_path, map_location=self.device))
        self.model.eval()
        print(f"Model loaded from {model_path}")
