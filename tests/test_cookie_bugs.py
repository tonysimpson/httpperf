def reply(wire, *set_cookies):
    head = b"HTTP/1.1 200 OK\r\n" + b"".join(b"Set-Cookie: " + c + b"\r\n" for c in set_cookies)
    wire.reply = head + b"Content-Length: 0\r\n\r\n"


async def sent_back(wire, s):
    await s.get(wire.url + "/set")
    wire.reply = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
    await s.get(wire.url + "/next")
    return next((line[8:] for line in wire.head().split("\r\n") if line.lower().startswith("cookie: ")), "")


async def test_two_set_cookies_with_a_quoted_equals_are_both_kept(wire, s):
    reply(wire, b'a="x=y"', b"b=2")
    assert await sent_back(wire, s) in ('a="x=y"; b=2', 'b=2; a="x=y"')


async def test_an_invalid_cookie_does_not_stop_the_rest(wire, s):
    reply(wire, b"a=1", b"=; bad", b"b,c=2", b"d=4")
    assert {"a=1", "d=4"} <= set((await sent_back(wire, s)).split("; "))


async def test_deleting_then_setting_a_cookie_keeps_the_new_value(wire, s):
    reply(wire, b"a=old; Expires=Thu, 01 Jan 1970 00:00:00 GMT", b"a=new")
    assert await sent_back(wire, s) == "a=new"


async def test_setting_then_deleting_a_cookie_removes_it(wire, s):
    reply(wire, b"a=new", b"a=old; Max-Age=0")
    assert await sent_back(wire, s) == ""


async def test_cookie_values_are_sent_back_unchanged(wire, s):
    reply(wire, b'a="q/v"', b"b=x/y", b"c=k=v")
    assert sorted((await sent_back(wire, s)).split("; ")) == ['a="q/v"', "b=x/y", "c=k=v"]
