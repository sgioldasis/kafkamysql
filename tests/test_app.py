from .context import kafkamysql


def test_app(capsys, example_fixture):
    # pylint: disable=W0612,W0613
    kafkamysql.KafkaMySql.run()
    captured = capsys.readouterr()

    assert "Hello World..." in captured.out
