"""Model loading with lazy singletons."""

import logging

from perch_hoplite.zoo.model_configs import load_model_by_name

log = logging.getLogger(__name__)

_whale_model = None


def get_whale_model():
    """Load (or return cached) Whale model (downloads on first run)."""
    global _whale_model
    if _whale_model is None:
        #log.info("Loading [bold]multispecies_whale[/bold] model...")
        log.info("Loading [bold]perch_v2[/bold] model...")
        #_whale_model = load_model_by_name("multispecies_whale")
        _whale_model = load_model_by_name("perch_v2")
        #log.info(
        #    "Model ready  sample_rate=[cyan]%d[/cyan]  classes=[cyan]%s[/cyan]",
        #    _whale_model.sample_rate,
        #    list(_whale_model.class_list.classes),
        #)
        #log.info("DEBUG: type of class_list = %s", type(_whale_model.class_list))
        #log.info("DEBUG: class_list contents = %s", _whale_model.class_list)
        #log.info("DEBUG: dir of class_list = %s", dir(_whale_model.class_list))
        log.info(
            "Model ready  sample_rate=[cyan]%d[/cyan]  classes=[cyan]%s[/cyan]",
            _whale_model.sample_rate,
            list(_whale_model.class_list.keys()) if isinstance(_whale_model.class_list, dict) else list(_whale_model.class_list.classes),
            )    
    return _whale_model
