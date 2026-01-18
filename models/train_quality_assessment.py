"""
Train Quality Assessment Model

Example script to train a quality assessment model for visual degradations.
"""

import numpy as np
import argparse
import torch
from torch.utils.data import DataLoader
from models.quality_assessment.trainer import QualityAssessmentTrainer, ImageQualityDataset


def main():
    """Main training function."""
    parser = argparse.ArgumentParser(description="Train quality assessment model")
    parser.add_argument("--train_images", type=str, required=True,
                       help="Path to training images (numpy array)")
    parser.add_argument("--train_labels", type=str, required=True,
                       help="Path to training labels (numpy array)")
    parser.add_argument("--output_path", type=str, default="./models/quality_assessment/quality_model.pth",
                       help="Path to save trained model")
    parser.add_argument("--epochs", type=int, default=10,
                       help="Number of training epochs (default: 10)")
    parser.add_argument("--batch_size", type=int, default=32,
                       help="Batch size (default: 32)")
    parser.add_argument("--lr", type=float, default=0.001,
                       help="Learning rate (default: 0.001)")
    parser.add_argument("--device", type=str, default="cpu",
                       help="Device to use: 'cpu' or 'cuda' (default: cpu)")
    
    args = parser.parse_args()
    
    # Load data
    print(f"Loading training data...")
    train_images = np.load(args.train_images)
    train_labels = np.load(args.train_labels)
    
    print(f"Training images shape: {train_images.shape}")
    print(f"Training labels shape: {train_labels.shape}")
    
    # Create dataset and dataloader
    dataset = ImageQualityDataset(train_images, train_labels)
    train_loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=True)
    
    # Train model
    print(f"Training quality assessment model...")
    trainer = QualityAssessmentTrainer(device=args.device)
    trainer.train(train_loader, epochs=args.epochs, lr=args.lr)
    
    # Save model
    trainer.save(args.output_path)
    print(f"Training completed. Model saved to {args.output_path}")


if __name__ == "__main__":
    main()
