"""Model loading with lazy singletons."""

import logging

from perch_hoplite.zoo.model_configs import load_model_by_name

log = logging.getLogger(__name__)

_whale_model = None


def get_whale_model():
    """Load (or return cached) Google Multispecies Whale model (downloads on first run)."""
    global _whale_model
    if _whale_model is None:
        log.info("Loading [bold]multispecies_whale[/bold] model...")
        _whale_model = load_model_by_name("multispecies_whale")
        log.info(
            "Model ready  sample_rate=[cyan]%d[/cyan]  classes=[cyan]%s[/cyan]",
            _whale_model.sample_rate,
            list(_whale_model.class_list.classes),
        )
    return _whale_model
