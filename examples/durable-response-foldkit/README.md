# Durable response with Foldkit

A Foldkit page for the [durable response](../durable-response) server. Its update function folds each batch of stream entries into the view, a subscription owns the reader, and a saved checkpoint lets a reload continue where it stopped. A stop from any tab reaches every reader. See [streamsy.dev](https://streamsy.dev) for the Streamsy documentation.
