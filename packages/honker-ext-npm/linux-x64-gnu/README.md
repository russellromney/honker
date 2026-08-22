# @russellthehippo/honker-ext-linux-x64-gnu

The Honker SQLite loadable extension (`libhonker_ext.so`) for `linux-x64-gnu`.

You do not install this directly. It arrives as an optional
dependency of `@russellthehippo/honker-node` or
`@russellthehippo/honker-bun`, which pick the right platform
automatically.

To load Honker onto a SQLite connection you already own:

```js
const { extensionPath } = require('@russellthehippo/honker-node/extension');
db.loadExtension(extensionPath());
db.prepare('SELECT honker_bootstrap()').run();
```

Full docs: https://honker.dev
