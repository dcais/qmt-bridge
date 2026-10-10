"""Environment selection must not silently start the wrong deployment."""
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from service.ipc_bridge.__main__ import main


def test_selected_environment_is_exclusive_and_process_values_win(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('.env').write_text('QMT_BRIDGE_ACCOUNT_ID=wrong-default\nQMT_BRIDGE_PORT=9999\n')
    for name, port in [('staging', 8888), ('prd', 8889)]:
        Path('.env.' + name).write_text(
            'QMT_BRIDGE_ID=' + name + '\nQMT_BRIDGE_ACCOUNT_ID=000123\n'
            'QMT_BRIDGE_PORT=' + str(port) + '\n')
        with patch.dict(os.environ, {}, clear=True), \
                patch('service.ipc_bridge.app.create_app') as create, \
                patch('uvicorn.run') as run:
            main(['--env-file', '.env.' + name])
            cfg = create.call_args.args[0]
            assert (cfg.bridge_id, cfg.account_id, cfg.port) == (name, '000123', port)
            assert run.call_args.kwargs['workers'] == 1
            os.environ['QMT_BRIDGE_PORT'] = '18999'
            main(['--env-file', '.env.' + name])
            assert create.call_args.args[0].port == 18999


def test_missing_explicit_environment_fails_before_start(tmp_path):
    with patch('uvicorn.run') as run, pytest.raises(SystemExit) as exc:
        main(['--env-file', str(tmp_path / '.env.missing')])
    assert exc.value.code == 2
    run.assert_not_called()


def test_default_environment_still_supported(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('.env').write_text('QMT_BRIDGE_ID=default-file\n')
    with patch.dict(os.environ, {}, clear=True), \
            patch('service.ipc_bridge.app.create_app') as create, patch('uvicorn.run'):
        main([])
        assert create.call_args.args[0].bridge_id == 'default-file'
